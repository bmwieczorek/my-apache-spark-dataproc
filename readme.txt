************************
# SPARK 3.1.3 - Cluster
************************

# Create cluster bucket
gsutil -m rm -r gs://${GCP_PROJECT}-bartek-spark-3-1-3-on-dataproc
gsutil mb -l ${GCP_REGION} gs://${GCP_PROJECT}-bartek-spark-3-1-3-on-dataproc

# Create dataproc cluster with spark logs in logs explorer
gcloud dataproc clusters delete bartek-spark-3-1-3-on-dataproc --project ${GCP_PROJECT} --region us-central1 --quiet
gcloud dataproc clusters create bartek-spark-3-1-3-on-dataproc \
--project ${GCP_PROJECT} --region us-central1 --zone="" --no-address \
--subnet ${GCP_SUBNETWORK} \
--master-machine-type t2d-standard-4 --master-boot-disk-size 1000 \
--num-workers 2 --worker-machine-type t2d-standard-4 --worker-boot-disk-size 2000 \
--image-version 2.0 \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--service-account=${GCP_SERVICE_ACCOUNT} \
--bucket ${GCP_PROJECT}-bartek-spark-3-1-3-on-dataproc \
--optional-components DOCKER \
--enable-component-gateway \
--properties spark:spark.master.rest.enabled=true,dataproc:dataproc.logging.stackdriver.job.driver.enable=true,dataproc:dataproc.logging.stackdriver.enable=true,dataproc:jobs.file-backed-output.enable=true,dataproc:dataproc.logging.stackdriver.job.yarn.container.enable=true \
--metric-sources=spark,hdfs,yarn,spark-history-server,hiveserver2,hivemetastore,monitoring-agent-defaults

# SPARK 3.1.3 - build and deploy
export JAVA_HOME=$(/usr/libexec/java_home -v1.8)
export PATH=$JAVA_HOME/bin:$PATH
mvn clean package -Pdist -Dspark.version=3.1.3 -Djava.version=1.8

# job finishes successfully after 11 secs when using spark built-in ConsoleSink
gcloud dataproc jobs submit spark --cluster=bartek-spark-3-1-3-on-dataproc --region=us-central1 \
--class=com.bawi.spark.MySimplestSparkApp \
--jars=target/my-apache-spark-dataproc-0.1-SNAPSHOT.jar \
--properties=spark.metrics.conf.*.sink.console.class=org.apache.spark.metrics.sink.ConsoleSink \
--labels=job_name=bartek-mysimplestsparkapp


# when replace above ConsoleSink with below MyConsoleSink
--properties=spark.metrics.conf.*.sink.myconsole.class=org.apache.spark.metrics.sink.MyConsoleSink \

# I get error below and spark job never finishes
Created MyAbstractConsoleSink with {class=org.apache.spark.metrics.sink.MyConsoleSink}
Created MyAbstractConsoleSink on bartek-spark-3-1-3-on-dataproc-m
Using Legacy Constructor required by MetricsSystem::registerSinks() for spark < 3.2
[2023-05-08 10:35:08.367]Container exited with a non-zero exit code 1. Error file: prelaunch.err.
  23/05/08 10:35:08 ERROR org.apache.spark.metrics.MetricsSystem: Sink class org.apache.spark.metrics.sink.MyConsoleSink cannot be instantiated
  Exception in thread "main" java.lang.reflect.UndeclaredThrowableException
  Caused by: java.lang.ClassNotFoundException: org.apache.spark.metrics.sink.MyConsoleSink

************************
# SPARK 3.3.0 - Cluster
************************

# Create cluster bucket
gsutil -m rm -r gs://${GCP_PROJECT}-bartek-spark-3-3-0-on-dataproc
gsutil mb -l ${GCP_REGION} gs://${GCP_PROJECT}-bartek-spark-3-3-0-on-dataproc

# Create dataproc cluster with spark logs in logs explorer
gcloud dataproc clusters delete bartek-spark-3-3-0-on-dataproc --project ${GCP_PROJECT} --region us-central1 --quiet
gcloud dataproc clusters create bartek-spark-3-3-0-on-dataproc \
--project ${GCP_PROJECT} --region us-central1 --zone="" --no-address \
--subnet ${GCP_SUBNETWORK} \
--master-machine-type t2d-standard-4 --master-boot-disk-size 1000 \
--num-workers 2 --worker-machine-type t2d-standard-4 --worker-boot-disk-size 2000 \
--image-version 2.1 \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--service-account=${GCP_SERVICE_ACCOUNT} \
--bucket ${GCP_PROJECT}-bartek-spark-3-3-0-on-dataproc \
--optional-components DOCKER \
--enable-component-gateway \
--properties spark:spark.master.rest.enabled=true,dataproc:dataproc.logging.stackdriver.job.driver.enable=true,dataproc:dataproc.logging.stackdriver.enable=true,dataproc:jobs.file-backed-output.enable=true,dataproc:dataproc.logging.stackdriver.job.yarn.container.enable=true \
--metric-sources=spark,hdfs,yarn,spark-history-server,hiveserver2,hivemetastore,monitoring-agent-defaults


# SPARK 3.3.0 - build and deploy
export JAVA_HOME=$(/usr/libexec/java_home -v11)
export PATH=$JAVA_HOME/bin:$PATH
mvn clean package -Pdist -Dspark.version=3.3.0 -Djava.version=11

# job finishes successfully after 13 secs when using spark built-in ConsoleSink
gcloud dataproc jobs submit spark --cluster=bartek-spark-3-3-0-on-dataproc --region=us-central1 \
--class=com.bawi.spark.MySimplestSparkApp \
--jars=target/my-apache-spark-dataproc-0.1-SNAPSHOT.jar \
--properties=spark.metrics.conf.*.sink.console.class=org.apache.spark.metrics.sink.ConsoleSink \
--labels=job_name=bartek-mysimplestsparkapp

# when replace above ConsoleSink with below MyConsoleSink
--properties=spark.metrics.conf.*.sink.myconsole.class=org.apache.spark.metrics.sink.MyConsoleSink \

# I get error below and spark job never finishes
Created MyAbstractConsoleSink with {class=org.apache.spark.metrics.sink.MyConsoleSink}
Created MyAbstractConsoleSink on bartek-spark-3-3-0-on-dataproc-m
[2023-05-08 10:35:14.924]Container exited with a non-zero exit code 1. Error file: prelaunch.err.
23/05/08 10:35:14 ERROR MetricsSystem: Sink class org.apache.spark.metrics.sink.MyConsoleSink cannot be instantiated
Caused by: java.lang.ClassNotFoundException: org.apache.spark.metrics.sink.MyConsoleSink

************************
# Spark Serverless
************************

# Create dependencies bucket
gsutil -m rm -r gs://${GCP_PROJECT}-bartek-spark-on-dataproc-deps
gsutil mb -l ${GCP_REGION} gs://${GCP_PROJECT}-bartek-spark-on-dataproc-deps

# build and deploy with code MySimplestSparkApp:14 and :15 uncommented - explicitly programmatically set sink as not possible in using --properties
# either
# sparkConf.set("spark.metrics.conf.*.sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink")
# or
#  sparkConf.set("spark.metrics.conf.*.sink.myconsole.class", "org.apache.spark.metrics.sink.MyConsoleSink")

export JAVA_HOME=$(/usr/libexec/java_home -v11)
export PATH=$JAVA_HOME/bin:$PATH
mvn clean package -Pdist -Dspark.version=3.3.0 -Djava.version=11
gcloud dataproc batches submit --project ${GCP_PROJECT} --region us-central1 spark \
 --batch mysimplestsparkapp-$RANDOM --class com.bawi.spark.MySimplestSparkApp --version 1.1 \
 --jars=target/my-apache-spark-dataproc-0.1-SNAPSHOT.jar \
 --subnet ${GCP_SUBNETWORK} --service-account ${GCP_SERVICE_ACCOUNT} \
 --history-server-cluster projects/${GCP_PROJECT}/regions/us-central1/clusters/bartek-persistent-history-server \
 --deps-bucket=gs://${GCP_PROJECT}-bartek-spark-on-dataproc-deps \
 --labels=job_name=bartek-mysimplestsparkapp


# The job never finishes - hopefully it will be killed
UCreated MyAbstractConsoleSink with {class=org.apache.spark.metrics.sink.MyConsoleSink}
Using Constructor required by MetricsSystem::registerSinks() for spark >= 3.2
5/8/23, 9:58:18 AM =============================================================

-- Counters --------------------------------------------------------------------
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.ExecutorAllocationManager.executors.numberExecutorsDecommissionUnfinished=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.ExecutorAllocationManager.executors.numberExecutorsExitedUnexpectedly=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.ExecutorAllocationManager.executors.numberExecutorsGracefullyDecommissioned=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.ExecutorAllocationManager.executors.numberExecutorsKilledByDriver=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.HiveExternalCatalog.fileCacheHits=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.HiveExternalCatalog.filesDiscovered=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.HiveExternalCatalog.hiveClientCalls=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.HiveExternalCatalog.parallelListingJobCount=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.HiveExternalCatalog.partitionsFetched=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.LiveListenerBus.numEventsPosted=7
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.LiveListenerBus.queue.appStatus.numDroppedEvents=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.LiveListenerBus.queue.dataprocEvent.numDroppedEvents=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.LiveListenerBus.queue.eventLog.numDroppedEvents=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.LiveListenerBus.queue.executorManagement.numDroppedEvents=0
app_name:MySimplestSparkApp$.app_id:app-20230508095808-0000.driver.LiveListenerBus.queue.shared.numDroppedEvents=0

but Logs Explorer show many:
Caused by: java.lang.ClassNotFoundException: org.apache.spark.metrics.sink.MyConsoleSink	at java.base/jdk.internal.loader.BuiltinClassLoader.loadClass(BuiltinClassLoader.java:581)	at java.base/jdk.internal.loader.ClassLoaders$AppClassLoader.loadClass(ClassLoaders.java:178)	at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:522)	at java.base/java.lang.Class.forName0(Native Method)	at java.base/java.lang.Class.forName(Class.java:398)	at org.apache.spark.util.Utils$.classForName(Utils.scala:218)	at org.apache.spark.metrics.MetricsSystem.$anonfun$registerSinks$1(MetricsSystem.scala:215)	at scala.collection.mutable.HashMap.$anonfun$foreach$1(HashMap.scala:149)	at scala.collection.mutable.HashTable.foreachEntry(HashTable.scala:237)	at scala.collection.mutable.HashTable.foreachEntry$(HashTable.scala:230)	at scala.collection.mutable.HashMap.foreachEntry(HashMap.scala:44)	at scala.collection.mutable.HashMap.foreach(HashMap.scala:149)	at org.apache.spark.metrics.MetricsSystem.registerSinks(MetricsSystem.scala:199)	at org.apache.spark.metrics.MetricsSystem.start(MetricsSystem.scala:103)	at org.apache.spark.SparkEnv$.create(SparkEnv.scala:389)	at org.apache.spark.SparkEnv$.createExecutorEnv(SparkEnv.scala:210)	at org.apache.spark.executor.CoarseGrainedExecutorBackend$.$anonfun$run$7(CoarseGrainedExecutorBackend.scala:476)	at org.apache.spark.deploy.SparkHadoopUtil$$anon$1.run(SparkHadoopUtil.scala:62)	at org.apache.spark.deploy.SparkHadoopUtil$$anon$1.run(SparkHadoopUtil.scala:61)	at java.base/java.security.AccessController.doPrivileged(Native Method)	at java.base/javax.security.auth.Subject.doAs(Subject.java:423)	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1899)	... 4 more

#need to cancel the job
gcloud dataproc batches cancel mysimplestsparkapp-6579 --project ${GCP_PROJECT} --region us-central1
