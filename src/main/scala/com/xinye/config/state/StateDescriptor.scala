package com.xinye.config.state


import com.xinye.base.Rule
import com.xinye.enums.RuleSateEnum
import com.xinye.operator.pojo.DynamicKey
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction

import scala.collection.mutable.{ArrayBuffer, Map}

object StateDescriptor {

  val dynamicFilterRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicFilterRule", classOf[Integer], classOf[Rule])

  val dynamicKeyedMapState = new MapStateDescriptor[Integer, Rule]("DynamicKeyedRule", classOf[Integer], classOf[Rule])

  val dynamicTransFormRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicTransFormRule", classOf[Integer], classOf[Rule])

  val dynamicAggregateRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicAggregateRule", classOf[Integer], classOf[Rule])

  //  val dynamicPostAggregateMetricMapState = new ListStateDescriptor[Map[String, Any]]("DynamicPostAggregateRule", classOf[Map[String, Any]])

  val dynamicPostAggregateRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicPostAggregateRule", classOf[Integer], classOf[Rule])

  val dynamicAlarmRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicAlarmRule", classOf[Integer], classOf[Rule])

  //  val alarmStreamState = new MapStateDescriptor[Int, (Long, Int, util.Map[String, Double])]("AlarmStream", classOf[Int], classOf[(Long, Int, util.Map[String, Double])])

  val metricByKeyState = new MapStateDescriptor("metricByKey", classOf[Long], classOf[ArrayBuffer[scala.collection.mutable.Map[String, Object]]])

  //  val detailByKeyState = new ListStateDescriptor("DetailMetrics", classOf[(Long, util.Map[String, Object])])

  //  val alarmByKeyState = new ListStateDescriptor("metricByKey", classOf[util.Map[String, Double]])

  val keyState = new ValueStateDescriptor[Int]("metricTag", classOf[Int], 0)

  val postState = new ValueStateDescriptor[Int]("postTag", classOf[Int], 0)

  def changeBroadcastState(value: Rule, ruleState: BroadcastState[Integer, Rule]): Unit = {
    RuleSateEnum.fromString(value.getRuleState) match {
      case RuleSateEnum.ACTIVE => ruleState.put(value.getRuleID, value)
      case RuleSateEnum.DELETE => ruleState.remove(value.getRuleID)
      case RuleSateEnum.PAUSE =>
        val rule = ruleState.get(value.getRuleID)
        rule.setRuleState(RuleSateEnum.PAUSE.toString)
        ruleState.put(rule.getRuleID, rule)
    }
  }

}
