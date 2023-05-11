# See and run ./redeploy-cluster.sh
Driver output:
Created MyGcpMetricSink with properties {class=org.apache.spark.metrics.sink.MyGcpMetricSink} on bartek-spark-313s-on-dataproc-m
Using Legacy Constructor required by MetricsSystem::registerSinks() for spark < 3.2
Starting MyGcpMetricSink with 60 SECONDS
Created MyGcpMetricSink with properties {class=org.apache.spark.metrics.sink.MyGcpMetricSink} on bartek-spark-313s-on-dataproc-m
Using Legacy Constructor required by MetricsSystem::registerSinks() for spark < 3.2
Starting MyGcpMetricSink with 60 SECONDS
23/05/11 09:02:25 INFO com.codahale.metrics.MyGcpMetricsReporter: [metrics-gcp-custom-metrics-reporter-3-thread-1:72] Publishing custom.googleapis.com/spark/MySimpleSparkAppWithMetrics.driver.CustomMetricSparkPlugin.driver_counter=0
23/05/11 09:03:13 INFO com.bawi.spark.MySimpleSparkAppWithMetrics: Cnt: 1000
23/05/11 09:03:13 INFO org.sparkproject.jetty.server.AbstractConnector: Stopped Spark@35088e87{HTTP/1.1, (http/1.1)}{0.0.0.0:0}
23/05/11 09:03:13 INFO com.codahale.metrics.MyGcpMetricsReporter: [main:1] Publishing custom.googleapis.com/spark/MySimpleSparkAppWithMetrics.driver.CustomMetricSparkPlugin.driver_counter=1000
Stopping MyGcpMetricSink
Stopping MyGcpMetricSink
Job [...] finished successfully.


Output from GCP logs explore:
TIMESTAMP             MESSAGE
2023-05-11T09:02:25Z  [metrics-gcp-custom-metrics-reporter-3-thread-1:72] Publishing custom.googleapis.com/spark/MySimpleSparkAppWithMetrics.driver.CustomMetricSparkPlugin.driver_counter=0
2023-05-11T09:02:31Z  [metrics-gcp-custom-metrics-reporter-1-thread-1:26] Publishing custom.googleapis.com/spark/MySimpleSparkAppWithMetrics.1.CustomMetricSparkPlugin.executor_counter=588
2023-05-11T09:02:32Z  [metrics-gcp-custom-metrics-reporter-1-thread-1:26] Publishing custom.googleapis.com/spark/MySimpleSparkAppWithMetrics.2.CustomMetricSparkPlugin.executor_counter=0
2023-05-11T09:02:33Z  [shutdown-hook-0:46] Publishing custom.googleapis.com/spark/MySimpleSparkAppWithMetrics.2.CustomMetricSparkPlugin.executor_counter=0
2023-05-11T09:02:33Z  [shutdown-hook-0:46] Exception publishing GCP metric custom.googleapis.com/spark/MySimpleSparkAppWithMetrics.2.CustomMetricSparkPlugin.executor_counter, message: io.grpc.StatusRuntimeException: INVALID_ARGUMENT: One or more TimeSeries could not be written: One or more points were written more frequently than the maximum sampling period configured for the metric.: gce_instance ...
2023-05-11T09:03:13Z  [main:1] Publishing custom.googleapis.com/spark/MySimpleSparkAppWithMetrics.driver.CustomMetricSparkPlugin.driver_counter=1000
2023-05-11T09:03:13Z  [CoarseGrainedExecutorBackend-stop-executor:48] Publishing custom.googleapis.com/spark/MySimpleSparkAppWithMetrics.1.CustomMetricSparkPlugin.executor_counter=1000
