package com.equitysim;

import com.equitysim.common.*;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.JSONObject;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;


public class TwitterFlowPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterFlowPipeline.class);
    static final int WINDOW_SIZE = 1;  // Default window duration in minutes
    private static final String TIMESTAMP_ATTRIBUTE = "created_at";

    private static DateTimeFormatter fmt =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));

    /**
     * Options supported by {@link WindowedWordCount}.
     *
     * <p>Inherits standard example configuration options, which allow specification of the BigQuery
     * table and the PubSub topic, as well as the {@link WordCount.WordCountOptions} support for
     * specification of the input file.
     */
    public static interface Options extends PubsubTopicOptions, BigQueryTableOptions {

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();
        void setInputFile(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();
        void setOutput(String value);

        @Description("Fixed window duration, in minutes")
        @Default.Integer(WINDOW_SIZE)
        Integer getWindowSize();
        void setWindowSize(Integer value);

        @Description("Whether to run the pipeline with unbounded input")
        boolean isUnbounded();
        void setUnbounded(boolean value);

        @Description("BigQuery Dataset to write tables to. Must already exist.")
        @Default.String("test1")
        String getDataset();
        void setDataset(String value);

        @Description("Prefix used for the BigQuery table names")
        @Default.String("hourly_question_totals")
        String getQuestionTotalsTableName();
        void setQuestionTotalsTableName(String value);

        @Description("Prefix used for the BigQuery table names")
        @Default.String("hourly_feedback_totals")
        String getFeedbackTotalsTableName();
        void setFeedbackTotalsTableName(String value);
    }


    /**
     * Create a map of information that describes how to write pipeline output to BigQuery. This map
     * is used to write team score sums and includes event timing information.
     */
    protected static Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>>
    configureFeedbackWindowedTableWrite() {

        Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
                new HashMap<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>>();
        tableConfigure.put(
                "widget_id",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "STRING", (c, w) -> c.element().getKey()));
        tableConfigure.put(
                "helpful_true",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "INTEGER", (c, w) -> c.element().getValue()));
        tableConfigure.put(
                "window_start",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "TIMESTAMP",
                        (c, w) -> {
                            IntervalWindow window = (IntervalWindow) w;
                            return ISODateTimeFormat.dateTime().print(window.start().toDateTime());
                        }));
        tableConfigure.put(
                "window_end",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "TIMESTAMP",
                        (c, w) -> {
                            IntervalWindow window = (IntervalWindow) w;
                            return ISODateTimeFormat.dateTime().print(window.end().toDateTime());
                        }));
        tableConfigure.put(
                "processing_time",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "STRING", (c, w) -> fmt.print(Instant.now())));
        tableConfigure.put(
                "timing",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "STRING", (c, w) -> c.pane().getTiming().toString()));
        return tableConfigure;
    }

    /**
     * Create a map of information that describes how to write pipeline output to BigQuery. This map
     * is used to write team score sums and includes event timing information.
     */
    protected static Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>>
    configureQuestionsWindowedTableWrite() {

        Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
                new HashMap<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>>();
        tableConfigure.put(
                "widget_id",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "STRING", (c, w) -> c.element().getKey()));
        tableConfigure.put(
                "num_questions",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "INTEGER", (c, w) -> c.element().getValue()));
        tableConfigure.put(
                "window_start",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "TIMESTAMP",
                        (c, w) -> {
                            IntervalWindow window = (IntervalWindow) w;
                            return ISODateTimeFormat.dateTime().print(window.start().toDateTime());
                        }));
        tableConfigure.put(
                "window_end",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "TIMESTAMP",
                        (c, w) -> {
                            IntervalWindow window = (IntervalWindow) w;
                            return ISODateTimeFormat.dateTime().print(window.end().toDateTime());
                        }));
        tableConfigure.put(
                "processing_time",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "STRING", (c, w) -> fmt.print(Instant.now())));
        tableConfigure.put(
                "timing",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
                        "STRING", (c, w) -> c.pane().getTiming().toString()));
        return tableConfigure;
    }

    /**
     * Create a map of information that describes how to write pipeline output to BigQuery. This map
     * is used to write team score sums and includes event timing information.
     */

    protected static Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Iterable<String>>>>
    configureQuidJsonWindowedTableWrite() {

        Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Iterable<String>>>> tableConfigure =
                new HashMap<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Iterable<String>>>>();
        tableConfigure.put(
                "quid",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Iterable<String>>>(
                        "STRING", (c, w) -> c.element().getKey()));
        tableConfigure.put(
                "event_obj_array",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Iterable<String>>>(
                        "STRING", (c, w) -> String.join("++",  c.element().toString() )));
        tableConfigure.put(
                "window_start",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Iterable<String>>>(
                        "TIMESTAMP",
                        (c, w) -> {
                            IntervalWindow window = (IntervalWindow) w;
                            return ISODateTimeFormat.dateTime().print(window.start().toDateTime());
                        }));
        tableConfigure.put(
                "window_end",
                new WriteWindowedToBigQuery.FieldInfo<KV<String, Iterable<String>>>(
                        "TIMESTAMP",
                        (c, w) -> {
                            IntervalWindow window = (IntervalWindow) w;
                            return ISODateTimeFormat.dateTime().print(window.end().toDateTime());
                        }));
        return tableConfigure;
    }

    /**
     * Class to hold info about tweet objects
     */
    @DefaultCoder(AvroCoder.class)
    public static class TweetObj {
        @Nullable String tweet_id;
        @Nullable Long timestamp;
        @Nullable JSONObject aidaEventObjJson;
        @Nullable String quid;

        public TweetObj() {}

        public TweetObj(String widget_id, JSONObject aidaEventObjJson, Long timestamp, String quid) {
            this.tweet_id = widget_id;
            this.timestamp = timestamp;
            this.aidaEventObjJson = aidaEventObjJson;
            this.quid = quid;
        }

        public String widget_id() {
            return this.tweet_id;
        }

        public JSONObject getAidaEventObjJson() { return aidaEventObjJson; }

        public Long getTimestamp() {
            return this.timestamp;
        }

        public String getWidget_id(){
            return this.tweet_id;
        }

        public String toString(){

            return aidaEventObjJson.toString(4);
        }


        public String getEventSubType(){

            String sub_type = aidaEventObjJson.getString("subType");
            return sub_type;
        }

        public int getFeedback(){

            String sub_type = aidaEventObjJson.getString("subType");

            if(sub_type.equals("Feedback"))
                return 1; else return 0;
        }

        public int getHelpfulFeedback(){
            Boolean helpful = aidaEventObjJson.getBoolean("helpful");

            if(helpful) return 1;else return 0;
        }

        public String getQuid(){
            String current_quid = aidaEventObjJson.getString("quid");

            return current_quid;
        }

    }

    /**
     * Creates a pcollection of widget_feedback_info objects from pubsub msgs
     */
    static class ProcessEachElement extends DoFn<String, TweetObj> {
        private static final Logger LOG = LoggerFactory.getLogger(ProcessEachElement.class);

        @ProcessElement
        public void processElement(ProcessContext c) {

            String msg = c.element();


            JSONObject tweet_json = new JSONObject(msg);
            try {
                String tweet_id = tweet_json.getString("id_str");
                Long event_timestamp = c.timestamp().getMillis();
                String tweet_txt = tweet_json.getString("text");

                TweetObj current_info_object = new TweetObj(tweet_id, tweet_json, event_timestamp, tweet_txt);
                LOG.debug("PROCESSING " + current_info_object.toString());
                c.output(current_info_object);
            }catch (Exception e){
                LOG.error("Parse error on " + c.element() + ", " + e.getMessage());
            }
        }
    }

    public static class ChangeEventToStringFn extends DoFn<TweetObj, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            TweetObj i = c.element();
            c.output( i.toString());
        }
    }


    public static class ConvertSessionedQuidToKV extends PTransform<PCollection<TweetObj>,PCollection<KV<String,Iterable<String>>>> {

        ConvertSessionedQuidToKV() { }

        @Override
        public PCollection<KV<String, Iterable<String>>> expand(
                PCollection<TweetObj> aidaEventObj) {

            return aidaEventObj
                    .apply(MapElements
                            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                            .via((TweetObj aida_feedback_event) -> KV.of(aida_feedback_event.getQuid(),
                                    aida_feedback_event.toString() )  ) )
                    .apply(GroupByKey.<String, String>create());
        }
    }

    public static class ExtractAndSumFeedbackTrueTotals extends PTransform<PCollection<TweetObj>,PCollection<KV<String,Integer>>> {

        ExtractAndSumFeedbackTrueTotals() { }

        @Override
        public PCollection<KV<String, Integer>> expand(
                PCollection<TweetObj> aidaEventObj) {

            return aidaEventObj
                    .apply(MapElements
                            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                            .via((TweetObj aida_feedback_event) -> KV.of(aida_feedback_event.getWidget_id(), aida_feedback_event.getHelpfulFeedback())))
                    .apply(Sum.integersPerKey());
        }
    }

    public static class ExtractAndSumFeedbackTotals extends PTransform<PCollection<TweetObj>,PCollection<KV<String,Integer>>> {

        ExtractAndSumFeedbackTotals() { }

        @Override
        public PCollection<KV<String, Integer>> expand(
                PCollection<TweetObj> aidaEventObj) {

            return aidaEventObj
                    .apply(MapElements
                            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                            .via((TweetObj aida_feedback_event) -> KV.of(aida_feedback_event.getWidget_id(), aida_feedback_event.getFeedback())))
                    .apply(Sum.integersPerKey());
        }
    }

    public static class ExtractAndSumQuestionTotals extends PTransform<PCollection<TweetObj>,PCollection<KV<String,Integer>>> {

        ExtractAndSumQuestionTotals() { }

        @Override
        public PCollection<KV<String, Integer>> expand(
                PCollection<TweetObj> aidaEventObj) {

            return aidaEventObj
                    .apply(MapElements
                            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                            .via((TweetObj aida_question_event) -> KV.of(aida_question_event.tweet_id, 1)))
                    .apply(Sum.integersPerKey());
        }
    }

    public static void main(String[] args) throws IOException {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        // Take input from pubsub and make pcollections of AidaEventObjects
        PCollection<String> pubSub_input = pipeline.apply(PubsubIO.readStrings().fromTopic(options.getPubsubTopic()))
                .apply("ParseTweetFromPubSub", ParDo.of(new ProcessEachElement()))
                .apply("AddEventTimestamps", WithTimestamps.of((TweetObj i) -> new Instant(i.getTimestamp()))
                        .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE))
                ).apply("WindowTweetIntoSeconds",
                        Window.<TweetObj>into(FixedWindows.of(Duration.standardSeconds(2)))
                                .triggering(AfterWatermark.pastEndOfWindow()
                                        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.standardSeconds(1)))
                                        .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.standardSeconds(2))))
                                .withAllowedLateness(Duration.millis(500))
                                .discardingFiredPanes()
                )
                .apply("ExtractTweetTxt",ParDo.of(new ChangeEventToStringFn()));

        pubSub_input.apply(new WriteOneFilePerWindow(options.getOutput(), 2));

//        // Take AidaEventObject pcollection and filter out non feedback events
//        PCollection<TweetObj> aida_feedback_events = pubSub_input.apply("FilterNonFeedback", Filter.by(
//                (TweetObj eObj)
//                        -> eObj.getEventSubType().equals("Feedback")  )
//        );


//        // Take feedback filtered aida events and create hourly windows
//        PCollection<TweetObj> windowed_aida_feedback_objects = aida_feedback_events
//                .apply("AddEventTimestamps", WithTimestamps.of((TweetObj i) -> new Instant(i.getTimestamp()))
//                        .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE))
//                ).apply("FixedHourlyWindows",
//                        Window.<TweetObj>into(FixedWindows.of(Duration.standardHours(1)))
//                                .triggering(AfterWatermark.pastEndOfWindow()
//                                        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
//                                                .plusDelayOf(Duration.standardMinutes(5)))
//                                        .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
//                                                .plusDelayOf(Duration.standardMinutes(10))))
//                                .withAllowedLateness(Duration.standardDays(300))
//                                .discardingFiredPanes()
//                );
//
//
//        // Sum feedback true windowed totals
//        PCollection<KV<String,Integer>> kv_aida_feedback_true = windowed_aida_feedback_objects
//                .apply("GetTotalWindowedNumTrueFeedbacks", new ExtractAndSumFeedbackTrueTotals());
//
//        kv_aida_feedback_true.apply(
//                "WriteHourlyFeedbackTrueSum",
//                new WriteToBigQuery<KV<String, Integer>>(
//                        options.as(GcpOptions.class).getProject(),
//                        options.getDataset(),
//                        options.getFeedbackTotalsTableName(),
//                        configureFeedbackWindowedTableWrite()));
//
//
//        // Sum feedback windowed totals
//        PCollection<KV<String,Integer>> kv_aida_feedback = windowed_aida_feedback_objects
//                .apply("GetTotalWindowedNumFeedbacks", new ExtractAndSumFeedbackTotals());
//
//        kv_aida_feedback.apply(
//                "WriteHourlyFeedbackSum",
//                new WriteToBigQuery<KV<String, Integer>>(
//                        options.as(GcpOptions.class).getProject(),
//                        options.getDataset(),
//                        options.getQuestionTotalsTableName(),
//                        configureQuestionsWindowedTableWrite()));


        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }

    }
}