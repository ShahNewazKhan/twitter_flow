#!/bin/bash
set -e

COMMAND=${4}
PROJECT_ID=${1}
BUCKET_NAME=${2}
TOPIC_ID=${3}

echo "${1} ${2} ${3} ${4}"
function usage
{
    echo "$ pipeline.sh PROJECT_ID BUCKET_NAME TOPIC_ID [run|options]"
}

if [[ -z $@ ]]; then
    usage
    exit 0
fi

case "$COMMAND" in
    options )
        mvn compile exec:java \
        -Dexec.mainClass=com.equitysim.TwitterFlowPipeline \
        -Dexec.args="--help=AidaLogAnalyticsPipelineOptions"
        ;;
    run )
        mvn compile exec:java \
        -Dexec.mainClass=com.equitysim.TwitterFlowPipeline \
        -Dexec.args="\
        --project=${PROJECT_ID} \
        --runner=DataflowRunner \
        --output=gs://${BUCKET_NAME}/output \
        --unbounded=true \
        --pubsubTopic=projects/${PROJECT_ID}/topics/${TOPIC_ID} \
        --tempLocation=gs://${BUCKET_NAME}/temp \
        --inputFile=\"\"" \
        -Pdataflow-runner        
        ;;
    direct-run )
        mvn compile exec:java \
        -Dexec.mainClass=com.equitysim.TwitterFlowPipeline \
        -Dexec.args="\
        --project=${PROJECT_ID} \
        --runner=DirectRunner \
        --output=gs://${BUCKET_NAME}/output \
        --unbounded=true \
        --pubsubTopic=projects/${PROJECT_ID}/topics/${TOPIC_ID} \
        --tempLocation=gs://${BUCKET_NAME}/temp \
        --inputFile=\"\"" \
        -Pdataflow-runner        
        ;;
    * )
        usage
        exit 1
        ;;
esac
