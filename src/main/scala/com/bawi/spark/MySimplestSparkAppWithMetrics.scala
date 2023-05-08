package com.bawi.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import com.codahale.metrics.Counter
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import java.util

object MySimplestSparkAppWithMetrics {
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val appName = getSimpleName(MySimplestSparkAppWithMetrics.getClass)

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.plugins", classOf[CustomMetricSparkPlugin].getName)
      .set("spark.metrics.namespace", appName)

    if (isLocal) {
      sparkConf.setMaster("local[*]")
    }

    //    sparkConf.set("spark.metrics.conf.*.sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink")
    //    sparkConf.set("spark.metrics.conf.*.sink.myconsole.class", "org.apache.spark.metrics.sink.MyConsoleSink")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val data = spark.sparkContext.parallelize(1 to 1000)
    val allData = data.map(n => {
      Thread.sleep(100)
      CustomMetricSparkPlugin.myCounter.inc(1)
      n
    })
    val cnt = allData.count()
    LOGGER.info(s"Cnt: $cnt")
    spark.stop()
  }

  private def isLocal: Boolean = {
    val osName = System.getProperty("os.name").toLowerCase
    osName.contains("mac") || osName.contains("windows")
  }

  def getClassName(clazz: Class[_]): String = {
    val name = clazz.getName
    name.substring(0, name.lastIndexOf("$"))
  }

  private def getSimpleName(clazz: Class[_]): String = {
    val name = clazz.getSimpleName
    name.substring(0, name.lastIndexOf("$"))
  }

  object CustomMetricSparkPlugin {
    val myCounter = new Counter
  }

  class CustomMetricSparkPlugin extends SparkPlugin {
    override def driverPlugin(): DriverPlugin = null

    override def executorPlugin(): ExecutorPlugin = new ExecutorPlugin {
      override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
        val metricRegistry = ctx.metricRegistry()
        metricRegistry.register("my_counter", CustomMetricSparkPlugin.myCounter)
      }
    }
  }
}
