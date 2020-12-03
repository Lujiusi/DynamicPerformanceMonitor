package com.xinye.config.state


import com.xinye.base.Rule
import com.xinye.enums.impl.RuleSateEnum
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

  val dynamicAggregateRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicAggregateRule", classOf[Integer], classOf[Rule])

  val detailState = new MapStateDescriptor[Long, ArrayBuffer[java.util.Map[String, String]]]("detailState", classOf[Long], classOf[ArrayBuffer[java.util.Map[String, String]]])

  detailState.enableTimeToLive(ttlConfig)

  val aggState = new MapStateDescriptor("aggState", classOf[String], classOf[Map[Long, Map[String, String]]])

  aggState.enableTimeToLive(ttlConfig)

  val dynamicPostAggregateRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicPostAggregateRule", classOf[Integer], classOf[Rule])

  val dynamicAlarmRuleMapState = new MapStateDescriptor[Integer, Rule]("DynamicAlarmRule", classOf[Integer], classOf[Rule])

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
