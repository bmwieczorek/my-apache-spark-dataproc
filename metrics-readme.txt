export JAVA_HOME=$(/usr/libexec/java_home -v1.8)
export PATH=$JAVA_HOME/bin:$PATH
mvn clean package -Pdist -Dspark.version=3.1.3 -Djava.version=1.8
gcloud dataproc jobs submit spark --cluster=bartek-spark-3-1-3-on-dataproc --region=us-central1 --class=com.bawi.spark.MySimplestSparkAppWithMetrics --jars=target/my-apache-spark-dataproc-0.1-SNAPSHOT.jar --properties=spark.metrics.conf.*.sink.slf4j.class=org.apache.spark.metrics.sink.Slf4jSink --labels=job_name=bartek-mysimplestsparkappwithmetrics
#gcloud dataproc jobs submit spark --cluster=bartek-spark-3-1-3-on-dataproc --region=us-central1 --class=com.bawi.spark.MySimplestSparkAppWithMetrics --jars=gs://${GCP_PROJECT}-bartek-dataproc/my-apache-spark-dataproc-0.1-SNAPSHOT.jar --properties=spark.metrics.conf.*.sink.slf4j.class=org.apache.spark.metrics.sink.Slf4jSink --labels=job_name=bartek-mysimplestsparkappwithmetrics

LABELS_JOB_NAME=bartek-mysimplestsparkappwithmetrics && \
CLUSTER_NAME=bartek-spark-3-1-3-on-dataproc && \
START_TIME="$(date -u -v-1M '+%Y-%m-%dT%H:%M:%SZ')" && \
END_TIME="$(date -u -v-60M '+%Y-%m-%dT%H:%M:%SZ')" && \
LATEST_JOB_ID=$(gcloud dataproc jobs list --region=us-central1 --filter="placement.clusterName=${CLUSTER_NAME} AND labels.job_name=${LABELS_JOB_NAME}" --format=json --sort-by=~status.stateStartTime | jq -r ".[0].reference.jobId") && \
echo "Latest job id: $LATEST_JOB_ID" &&
gcloud logging read --project ${GCP_PROJECT} "timestamp<=\"${START_TIME}\" AND timestamp>=\"${END_TIME}\" AND resource.type=cloud_dataproc_job AND labels.\"dataproc.googleapis.com/cluster_name\"=${CLUSTER_NAME} AND resource.labels.job_id=${LATEST_JOB_ID} AND jsonPayload.class=\"metrics\" AND jsonPayload.message=~\".*plugin.*\"" --format "table(timestamp,jsonPayload.message)" --order=asc

Latest job id: e739d8b7d2844642bc9....
TIMESTAMP             MESSAGE
2023-05-08T15:41:01Z  type=COUNTER, name=MySimplestSparkAppWithMetrics.2.plugin.com.bawi.spark.MySimplestSparkAppWithMetrics$CustomMetricSparkPlugin.my_counter, count=0
2023-05-08T15:41:01Z  type=COUNTER, name=MySimplestSparkAppWithMetrics.1.plugin.com.bawi.spark.MySimplestSparkAppWithMetrics$CustomMetricSparkPlugin.my_counter, count=182
2023-05-08T15:41:11Z  type=COUNTER, name=MySimplestSparkAppWithMetrics.2.plugin.com.bawi.spark.MySimplestSparkAppWithMetrics$CustomMetricSparkPlugin.my_counter, count=0
2023-05-08T15:41:11Z  type=COUNTER, name=MySimplestSparkAppWithMetrics.1.plugin.com.bawi.spark.MySimplestSparkAppWithMetrics$CustomMetricSparkPlugin.my_counter, count=380
2023-05-08T15:41:21Z  type=COUNTER, name=MySimplestSparkAppWithMetrics.2.plugin.com.bawi.spark.MySimplestSparkAppWithMetrics$CustomMetricSparkPlugin.my_counter, count=0
2023-05-08T15:41:21Z  type=COUNTER, name=MySimplestSparkAppWithMetrics.1.plugin.com.bawi.spark.MySimplestSparkAppWithMetrics$CustomMetricSparkPlugin.my_counter, count=580
2023-05-08T15:41:31Z  type=COUNTER, name=MySimplestSparkAppWithMetrics.2.plugin.com.bawi.spark.MySimplestSparkAppWithMetrics$CustomMetricSparkPlugin.my_counter, count=0
2023-05-08T15:41:31Z  type=COUNTER, name=MySimplestSparkAppWithMetrics.1.plugin.com.bawi.spark.MySimplestSparkAppWithMetrics$CustomMetricSparkPlugin.my_counter, count=780
2023-05-08T15:41:41Z  type=COUNTER, name=MySimplestSparkAppWithMetrics.2.plugin.com.bawi.spark.MySimplestSparkAppWithMetrics$CustomMetricSparkPlugin.my_counter, count=0
2023-05-08T15:41:41Z  type=COUNTER, name=MySimplestSparkAppWithMetrics.1.plugin.com.bawi.spark.MySimplestSparkAppWithMetrics$CustomMetricSparkPlugin.my_counter, count=980
2023-05-08T15:41:42Z  type=COUNTER, name=MySimplestSparkAppWithMetrics.2.plugin.com.bawi.spark.MySimplestSparkAppWithMetrics$CustomMetricSparkPlugin.my_counter, count=0
2023-05-08T15:41:42Z  type=COUNTER, name=MySimplestSparkAppWithMetrics.1.plugin.com.bawi.spark.MySimplestSparkAppWithMetrics$CustomMetricSparkPlugin.my_counter, count=1000
