package com.bawi.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object MySimplestSparkApp {
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(MySimplestSparkApp.getClass.getSimpleName)
    if (isLocal) sparkConf.setMaster("local[*]")

//    sparkConf.set("spark.metrics.conf.*.sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink")
//    sparkConf.set("spark.metrics.conf.*.sink.myconsole.class", "org.apache.spark.metrics.sink.MyConsoleSink")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val data = spark.sparkContext.parallelize(Array(1, 2, 4, 3, 5))
    val allData = data.map(n => {
      LOGGER.info(s"all element=$n"); n
    })
    val cnt = allData.count()
    LOGGER.info(s"Cnt: $cnt")
    spark.stop()
  }

  private def isLocal: Boolean = {
    val osName = System.getProperty("os.name").toLowerCase
    osName.contains("mac") || osName.contains("windows")
  }
}
