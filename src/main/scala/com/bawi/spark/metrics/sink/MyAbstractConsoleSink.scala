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

package com.bawi.spark.metrics.sink

import com.codahale.metrics.{MetricRegistry, MyConsoleReporter}

import java.util.concurrent.TimeUnit
import java.util.{Locale, Properties}

abstract class MyAbstractConsoleSink(property: Properties, registry: MetricRegistry) {
  System.out.println("Created MyAbstractConsoleSink with " + property)

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

  checkMinimalPollingPeriod(pollUnit, pollPeriod)

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

  def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int): Unit = {
    val period = TimeUnit.SECONDS.convert(pollPeriod, pollUnit)
    if (period < 1) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }
}
