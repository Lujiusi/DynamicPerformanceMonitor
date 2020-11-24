package com.xinye.config

import org.apache.flink.metrics.Gauge

class RuleGauge extends Gauge[Int] {

  private var value = 0

  def setValue(value: Int) {
    this.value = value
  }

  override def getValue: Int = {
    value
  }

}