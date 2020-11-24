package com.xinye.operator

import com.xinye.base.Rule
import com.xinye.operator.pojo.DynamicKey
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.Map

object OutPutUtils {

  final val dynamicFilterRuleOutput = new OutputTag[Rule]("DynamicFilterRule")

  final val dynamicKeyedRuleOutput = new OutputTag[Rule]("DynamicKeyedRule")

  final val dynamicTransFormRuleOutput = new OutputTag[Rule]("DynamicTransFormRule")

  final val dynamicAggDetailsOutput = new OutputTag[(Long, DynamicKey, Map[String, Object])]("DynamicAggDetails")

  final val clickHouseOutput = new OutputTag[String]("Sink2Clickhouse")

  final val druidOutput = new OutputTag[Map[String, Object]]("Sink2Druid")

}
