export JAVA_HOME=$(/usr/libexec/java_home -v1.8)
export PATH=$JAVA_HOME/bin:$PATH
mvn clean package -Pdist


export JAVA_HOME=$(/usr/libexec/java_home -v11)
export PATH=$JAVA_HOME/bin:$PATH
mvn clean package -Pdist

gcloud dataproc jobs submit spark --cluster=bartek-spark-on-dataproc --region=us-central1 \
--class=com.bawi.spark.MySimplestSparkApp \
--jars=target/my-apache-spark-dataproc-0.1-SNAPSHOT.jar \
--labels=job_name=bartek-mysimplestsparkapp


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
