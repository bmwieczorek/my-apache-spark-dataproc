#!/bin/bash

gcloud dataproc clusters delete bartek-spark-313s-on-dataproc --project ${GCP_PROJECT} --region us-central1 --quiet
gsutil -m rm -r gs://${GCP_PROJECT}-bartek-spark-313s-on-dataproc
gsutil mb -l ${GCP_REGION} gs://${GCP_PROJECT}-bartek-spark-313s-on-dataproc

export JAVA_HOME=$(/usr/libexec/java_home -v11)
export PATH=$JAVA_HOME/bin:$PATH
mvn clean package -Pdist -Dspark.version=3.1.3 -Djava.version=1.8

gsutil cp install-spark-metrics-sink-on-dataproc.sh  gs://${GCP_PROJECT}-bartek-spark-313s-on-dataproc/
gsutil cp target/my-apache-spark-metrics-sink-0.1-SNAPSHOT.jar gs://${GCP_PROJECT}-bartek-spark-313s-on-dataproc/

gcloud dataproc clusters create bartek-spark-313s-on-dataproc \
--project ${GCP_PROJECT} --region us-central1 --zone="" --no-address \
--subnet ${GCP_SUBNETWORK} \
--master-machine-type t2d-standard-4 --master-boot-disk-size 1000 \
--num-workers 2 --worker-machine-type t2d-standard-4 --worker-boot-disk-size 2000 \
--image-version 2.0 \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--service-account=${GCP_SERVICE_ACCOUNT} \
--bucket ${GCP_PROJECT}-bartek-spark-313s-on-dataproc \
--optional-components DOCKER \
--enable-component-gateway \
--initialization-actions gs://${GCP_PROJECT}-bartek-spark-313s-on-dataproc/install-spark-metrics-sink-on-dataproc.sh \
--metadata spark-metrics-sink-jar-gcs-path=gs://${GCP_PROJECT}-bartek-spark-313s-on-dataproc/my-apache-spark-metrics-sink-0.1-SNAPSHOT.jar \
--metadata spark-metrics-sink-jar-name=my-apache-spark-metrics-sink.jar \
--properties spark:spark.master.rest.enabled=true,dataproc:dataproc.logging.stackdriver.job.driver.enable=true,dataproc:dataproc.logging.stackdriver.enable=true,dataproc:jobs.file-backed-output.enable=true,dataproc:dataproc.logging.stackdriver.job.yarn.container.enable=true \
--metric-sources=spark,hdfs,yarn,spark-history-server,hiveserver2,hivemetastore,monitoring-agent-defaults

gcloud dataproc jobs submit spark --cluster=bartek-spark-313s-on-dataproc --region=us-central1 \
--class=com.bawi.spark.MyReadAvroGcsAndWriteBQBroadcastApp \
--jars=../my-apache-spark-3-scala/target/my-apache-spark-3-scala-0.1-SNAPSHOT.jar \
--labels=job_name=bartek-myreadavrogcsandwritebqbroadcastapp \
--properties ^#^spark.jars.packages=org.apache.spark:spark-avro_2.12:3.1.3,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0#spark.dynamicAllocation.enabled=true#spark.shuffle.service.enabled=true#spark.metrics.conf.*.sink.mygcpmetric.class=org.apache.spark.metrics.sink.MyGcpMetricSink \
-- \
 --projectId=${GCP_PROJECT}

gcloud dataproc jobs submit spark --cluster=bartek-spark-313s-on-dataproc --region=us-central1 \
--class=com.bawi.spark.MyMultiOutputMetricsApp \
--jars=../my-apache-spark-3-scala/target/my-apache-spark-3-scala-0.1-SNAPSHOT.jar \
--labels=job_name=bartek-mymultioutputmetricsapp \
--properties ^#^spark.jars.packages=org.apache.spark:spark-avro_2.12:3.1.3#spark.dynamicAllocation.enabled=true#spark.shuffle.service.enabled=true#spark.metrics.conf.*.sink.myconsole.class=org.apache.spark.metrics.sink.MyConsoleSink \
-- \
 --projectId=${GCP_PROJECT}


