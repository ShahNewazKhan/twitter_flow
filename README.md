# EQUITYSIM-TWITER-FLOW

This repo houses the Equity Sim Twitter analytics Java pipeline for GCP Dataflow

## Usage

Setup mvn project with com.equitysim package

```sh
mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=2.1.0 \
      -DgroupId=com.equitysim \
      -DartifactId=twitter_flow \
      -Dversion="0.1" \
      -Dpackage=com.equitysim \
      -DinteractiveMode=false
```

Build and run twitter_flow pipeline locally

```sh
mvn compile exec:java \
    -Dexec.mainClass=com.equitysim.TwitterFlowPipeline \
    -Dexec.args="--output=./output/"
``` 

Build and run twitter_flow pipeline on GCP Dataflow

```sh
mvn compile exec:java \
        -Dexec.mainClass=com.equitysim.TwitterFlowPipeline \
        -Dexec.args="\
        --project=${PROJECT_ID} \
        --stagingLocation=gs://${BUCKET_NAME}/staging \
        --runner=DataflowRunner \
        --output=gs://${BUCKET_NAME}/output "
```

Build and run twitter_flow pipeline on GCP Dataflow using script

```sh
./pipeline.sh chc-admin twitter-flow fintech-tweet run

gcloud beta dataflow jobs list --status=(active|terminated|all)
```
