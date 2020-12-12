package com.xinye.state

import com.xinye.base.Rule
import com.xinye.enums.impl.RuleSateEnum
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time

import java.util.Map
import scala.collection.mutable.ArrayBuffer

object StateDescriptor {

  val ttlConfig: StateTtlConfig = StateTtlConfig.newBuilder(Time.hours(2))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .cleanupFullSnapshot()
    .build()

  val ruleState = new MapStateDescriptor[Integer, Rule]("RuleState", classOf[Integer], classOf[Rule])

  val detailState = new MapStateDescriptor[String, Map[Long, ArrayBuffer[Map[String, String]]]]("detailState", classOf[String], classOf[Map[Long, ArrayBuffer[Map[String, String]]]])

  detailState.enableTimeToLive(ttlConfig)

  val aggState = new MapStateDescriptor("aggState", classOf[String], classOf[Map[Long, Map[String, String]]])

  aggState.enableTimeToLive(ttlConfig)

  def changeBroadcastState(value: Rule, ruleState: BroadcastState[Integer, Rule]): Boolean = {
    if (value != null) {
      RuleSateEnum.fromString(value.getRuleState) match {
        case RuleSateEnum.START => ruleState.put(value.getRuleID, value)
        case RuleSateEnum.DELETE => ruleState.remove(value.getRuleID)
        case RuleSateEnum.STOP =>
          val rule = ruleState.get(value.getRuleID)
          rule.setRuleState(RuleSateEnum.STOP.toString)
          ruleState.put(rule.getRuleID, rule)
      }
      true
    } else {
      false
    }
  }

}