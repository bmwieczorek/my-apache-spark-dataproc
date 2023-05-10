package com.bawi.spark

import com.codahale.metrics.Counter
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.Collections

object MySimpleSparkAppWithMetrics {
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName.split('$')(0))

  def main(args: Array[String]): Unit = {
    val appName = MySimpleSparkAppWithMetrics.getClass.getSimpleName.split('$')(0)

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.plugins", classOf[CustomMetricSparkPlugin].getName)
      .set("spark.metrics.namespace", appName)
//      .set("spark.metrics.conf.master.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
//      .set("spark.metrics.conf.worker.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
//      .set("spark.metrics.conf.driver.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
//      .set("spark.metrics.conf.executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")

    if (isLocal) {
      sparkConf.setMaster("local[*]")
    //    sparkConf.set("spark.metrics.conf.*.sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink")
        sparkConf.set("spark.metrics.conf.*.sink.myconsole.class", "org.apache.spark.metrics.sink.MyConsoleSink")
    }

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val counterAccum = spark.sparkContext.longAccumulator("counterAccum")

    val data = spark.sparkContext.parallelize(1 to 1000)
    val allData = data.map(n => {
      Thread.sleep(200)
      CustomMetricSparkPlugin.executorCounter.inc(1)
      counterAccum.add(1)
      n
    })
    val cnt = allData.count()

    CustomMetricSparkPlugin.driverCounter.inc(counterAccum.value)
    LOGGER.info(s"Cnt: $cnt")

    // wait to populate driver counter
    // Thread.sleep(50 * 1000)

    spark.stop()
  }

  private def isLocal: Boolean = {
    val osName = System.getProperty("os.name").toLowerCase
    osName.contains("mac") || osName.contains("windows")
  }

  object CustomMetricSparkPlugin {
    val driverCounter = new Counter
    val executorCounter = new Counter
  }

  class CustomMetricSparkPlugin extends SparkPlugin {
    override def driverPlugin(): DriverPlugin = new DriverPlugin {
      override def init(sparkContext: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
        val metricRegistry = pluginContext.metricRegistry()
        metricRegistry.register("driver_counter", CustomMetricSparkPlugin.driverCounter)
        Collections.emptyMap[String, String]
      }
    }

    override def executorPlugin(): ExecutorPlugin = new ExecutorPlugin {
      override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
        val metricRegistry = ctx.metricRegistry()
        metricRegistry.register("executor_counter", CustomMetricSparkPlugin.executorCounter)
      }
    }
  }
}
