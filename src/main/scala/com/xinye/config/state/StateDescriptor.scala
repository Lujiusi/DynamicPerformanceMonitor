package com.xinye.config.state


import com.xinye.base.Rule
import com.xinye.enums.RuleSateEnum
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, StateTtlConfig, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time

import java.util.Map
import scala.collection.mutable.ArrayBuffer

object StateDescriptor {

  val ttlConfig: StateTtlConfig = StateTtlConfig.newBuilder(Time.hours(2))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .cleanupFullSnapshot()
    .build()

  val dynamicFilterRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicFilterRule", classOf[Integer], classOf[Rule])

  val dynamicKeyedMapState = new MapStateDescriptor[Integer, Rule]("DynamicKeyedRule", classOf[Integer], classOf[Rule])

  val dynamicTransFormRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicTransFormRule", classOf[Integer], classOf[Rule])

  val dynamicAggregateRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicAggregateRule", classOf[Integer], classOf[Rule])

  val metricByKeyState = new MapStateDescriptor("metricByKey", classOf[String], classOf[java.util.Map[Long, ArrayBuffer[java.util.Map[String, String]]]])

  metricByKeyState.enableTimeToLive(ttlConfig)

  val dynamicPostAggregateRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicPostAggregateRule", classOf[Integer], classOf[Rule])

  val dynamicAlarmRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicAlarmRule", classOf[Integer], classOf[Rule])

  val timeToAliasName = new MapStateDescriptor[Long, Map[String, Map[String, String]]]("TimeToAliasName", classOf[Long], classOf[Map[String, Map[String, String]]])

  val aliasNameToGroup = new MapStateDescriptor[String, String]("AliasNameToGroup", classOf[String], classOf[String])

  //  val alarmStreamState = new MapStateDescriptor[Int, (Long, Int, util.Map[String, Double])]("AlarmStream", classOf[Int], classOf[(Long, Int, util.Map[String, Double])])

  //  val detailByKeyState = new ListStateDescriptor("DetailMetrics", classOf[(Long, util.Map[String, Object])])

  //  val alarmByKeyState = new ListStateDescriptor("metricByKey", classOf[util.Map[String, Double]])

  val keyState = new ValueStateDescriptor[Int]("metricTag", classOf[Int], 0)

  keyState.enableTimeToLive(ttlConfig)

  val postState = new ValueStateDescriptor[Int]("postTag", classOf[Int], 0)

  def changeBroadcastState(value: Rule, ruleState: BroadcastState[Integer, Rule]): Unit = {
    RuleSateEnum.fromString(value.getRuleState) match {
      case RuleSateEnum.START => ruleState.put(value.getRuleID, value)
      case RuleSateEnum.DELETE => ruleState.remove(value.getRuleID)
      case RuleSateEnum.STOP =>
        val rule = ruleState.get(value.getRuleID)
        rule.setRuleState(RuleSateEnum.STOP.toString)
        ruleState.put(rule.getRuleID, rule)
    }
  }

}
