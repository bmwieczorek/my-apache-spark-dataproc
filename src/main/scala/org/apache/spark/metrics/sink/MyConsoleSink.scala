package org.apache.spark.metrics.sink

import com.bawi.spark.metrics.sink.MyAbstractConsoleSink
import com.codahale.metrics.MetricRegistry
import org.apache.spark.SecurityManager

import java.util.Properties

class MyConsoleSink(property: Properties, registry: MetricRegistry)
  extends MyAbstractConsoleSink(property, registry) with Sink {

  def this(property: Properties, registry: MetricRegistry, securityMgr: SecurityManager) = {
    this(property, registry)
    System.out.println("Using Legacy Constructor required by MetricsSystem::registerSinks() for spark < 3.2")
  }
}
