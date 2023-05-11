/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.metrics.sink

import com.codahale.metrics.{MetricRegistry, MyGcpMetricsReporter}
import org.apache.spark.SecurityManager
import org.apache.spark.metrics.MetricsSystem

import java.net.InetAddress
import java.util.concurrent.TimeUnit
import java.util.{Locale, Properties}

//noinspection ScalaUnusedSymbol
class MyGcpMetricSink(properties: Properties, registry: MetricRegistry) extends Sink {

  def this(properties: Properties, registry: MetricRegistry, securityMgr: SecurityManager) = {
    this(properties, registry)
    System.out.println("Using Legacy Constructor required by MetricsSystem::registerSinks() for spark < 3.2")
  }

  // need to use System.out.println as Sl4j LOGGER is not working in this class on dataproc cluster
  System.out.println("Created MyGcpMetricSink with properties " + properties + " on " + InetAddress.getLocalHost.getHostName)

  private val DEFAULT_PERIOD = 60
  private val DEFAULT_UNIT = "SECONDS"

  private val PROPERTY_KEY_PERIOD = "period"
  private val PROPERTY_KEY_UNIT = "unit"

  val pollPeriod: Int = Option(properties.getProperty(PROPERTY_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(properties.getProperty(PROPERTY_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
    case None => TimeUnit.valueOf(DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val reporter: MyGcpMetricsReporter = new MyGcpMetricsReporter(registry)

  override def start(): Unit = {
    System.out.println("Starting " + getClass.getSimpleName + " with " + pollPeriod + " " + pollUnit)
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop(): Unit = {
    System.out.println("Stopping " + getClass.getSimpleName)
    reporter.stop()
  }

  override def report(): Unit = {
    reporter.report()
  }
}
