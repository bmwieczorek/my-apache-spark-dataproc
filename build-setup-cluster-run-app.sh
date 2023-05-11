#!/bin/bash

CLUSTER=bartek-spark-313s-on-dataproc
JAR_WITH_VERSION=my-apache-spark-metrics-sink-0.1-SNAPSHOT.jar
JAR_WITHOUT_VERSION=my-apache-spark-metrics-sink.jar

gcloud dataproc clusters delete ${CLUSTER} --project ${GCP_PROJECT} --region us-central1 --quiet
gsutil -m rm -r gs://${GCP_PROJECT}-${CLUSTER}
gsutil mb -l ${GCP_REGION} gs://${GCP_PROJECT}-${CLUSTER}

JAVA_HOME=$(/usr/libexec/java_home -v1.8)
export JAVA_HOME=$JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH
mvn clean package -Pdist -Dspark.version=3.1.3 -Djava.version=1.8

gsutil cp install-spark-metrics-sink-on-dataproc.sh  gs://${GCP_PROJECT}-${CLUSTER}/
gsutil cp target/${JAR_WITH_VERSION} gs://${GCP_PROJECT}-${CLUSTER}/

gcloud dataproc clusters create ${CLUSTER} \
--project ${GCP_PROJECT} --region us-central1 --zone="" --no-address \
--subnet ${GCP_SUBNETWORK} \
--master-machine-type t2d-standard-4 --master-boot-disk-size 1000 \
--num-workers 2 --worker-machine-type t2d-standard-4 --worker-boot-disk-size 2000 \
--image-version 2.0 \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--service-account=${GCP_SERVICE_ACCOUNT} \
--bucket ${GCP_PROJECT}-${CLUSTER} \
--optional-components DOCKER \
--enable-component-gateway \
--initialization-actions gs://${GCP_PROJECT}-${CLUSTER}/install-spark-metrics-sink-on-dataproc.sh \
--metadata spark-metrics-sink-jar-gcs-path=gs://${GCP_PROJECT}-${CLUSTER}/${JAR_WITH_VERSION} \
--metadata spark-metrics-sink-jar-name=${JAR_WITHOUT_VERSION} \
--properties spark:spark.master.rest.enabled=true,dataproc:dataproc.logging.stackdriver.job.driver.enable=true,dataproc:dataproc.logging.stackdriver.enable=true,dataproc:jobs.file-backed-output.enable=true,dataproc:dataproc.logging.stackdriver.job.yarn.container.enable=true \
--metric-sources=spark,hdfs,yarn,spark-history-server,hiveserver2,hivemetastore,monitoring-agent-defaults

gcloud dataproc jobs submit spark --cluster=${CLUSTER} --region=us-central1 \
--class=com.bawi.spark.MySimpleSparkAppWithMetrics \
--jars=target/${JAR_WITH_VERSION} \
--labels=job_name=bartek-mysimplesparkappwithmetrics \
--properties ^#^spark.dynamicAllocation.enabled=true#spark.shuffle.service.enabled=true#spark.metrics.conf.*.sink.mygcpmetric.class=org.apache.spark.metrics.sink.MyGcpMetricSink \
-- \
 --projectId=${GCP_PROJECT}


echo "Waiting 10 secs for logs to appear in GCP Logs Explorer"
SLEEP 10

CLUSTER=bartek-spark-313s-on-dataproc
LABELS_JOB_NAME=bartek-mysimplesparkappwithmetrics && \
START_TIME="$(date -u -v-1S '+%Y-%m-%dT%H:%M:%SZ')" && \
END_TIME="$(date -u -v-10M '+%Y-%m-%dT%H:%M:%SZ')" && \
LATEST_JOB_ID=$(gcloud dataproc jobs list --region=us-central1 --filter="placement.clusterName=${CLUSTER} AND labels.job_name=${LABELS_JOB_NAME}" --format=json --sort-by=~status.stateStartTime | jq -r ".[0].reference.jobId") && \
echo "Latest job id: $LATEST_JOB_ID" &&
gcloud logging read --project ${GCP_PROJECT} "timestamp<=\"${START_TIME}\" AND timestamp>=\"${END_TIME}\" AND resource.type=cloud_dataproc_job AND labels.\"dataproc.googleapis.com/cluster_name\"=${CLUSTER} AND resource.labels.job_id=${LATEST_JOB_ID} AND jsonPayload.message=~\".*MySimpleSparkAppWithMetrics.*\"" --format "table(timestamp,jsonPayload.message)" --order=asc
