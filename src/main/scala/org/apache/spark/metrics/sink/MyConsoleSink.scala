package org.apache.spark.metrics.sink

import com.codahale.metrics.{MetricRegistry, MyConsoleReporter}
import org.apache.spark.SecurityManager
import org.apache.spark.metrics.MetricsSystem

import java.net.InetAddress
import java.util.concurrent.TimeUnit
import java.util.{Locale, Properties}

class MyConsoleSink(property: Properties, registry: MetricRegistry) extends Sink {

  def this(property: Properties, registry: MetricRegistry, securityMgr: SecurityManager) = {
    this(property, registry)
    System.out.println("Using Legacy Constructor required by MetricsSystem::registerSinks() for spark < 3.2")
  }

  System.out.println("Created MyConsoleSink with " + property)
  private val hostName = InetAddress.getLocalHost.getHostName
  System.out.println("Created MyConsoleSink on " + hostName)

  val CONSOLE_DEFAULT_PERIOD = 10
  val CONSOLE_DEFAULT_UNIT = "SECONDS"

  val CONSOLE_KEY_PERIOD = "period"
  val CONSOLE_KEY_UNIT = "unit"

  val pollPeriod = Option(property.getProperty(CONSOLE_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => CONSOLE_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(CONSOLE_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
    case None => TimeUnit.valueOf(CONSOLE_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val reporter: MyConsoleReporter = MyConsoleReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build()

  def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
  }

  def stop(): Unit = {
    reporter.stop()
  }

  def report(): Unit = {
    reporter.report()
  }
}
