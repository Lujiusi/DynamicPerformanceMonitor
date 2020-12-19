package com.xinye.state

import com.xinye.base.Rule
import com.xinye.enums.impl.RuleStateEnum
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import com.alibaba.fastjson.JSONObject

import java.util
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

  val allDsToMaxWindow = new MapStateDescriptor("allDsToMaxWindow", classOf[Int], classOf[Iterable[Rule.AggregatorFun]])

  def changeBroadcastState(value: Rule, ruleState: BroadcastState[Integer, Rule]): Boolean = {
    if (value != null) {
      RuleStateEnum.fromString(value.getRuleState) match {
        case RuleStateEnum.START => ruleState.put(value.getRuleID, value)
        case RuleStateEnum.DELETE => ruleState.remove(value.getRuleID)
        case RuleStateEnum.STOP =>
          val rule = ruleState.get(value.getRuleID)
          rule.setRuleState(RuleStateEnum.STOP.toString)
          ruleState.put(rule.getRuleID, rule)
      }
      true
    } else {
      false
    }
  }

  val customs = new MapStateDescriptor[String, Map[Map[String, String], Int]]("customs", classOf[String], classOf[Map[Map[String, String], Int]])

}