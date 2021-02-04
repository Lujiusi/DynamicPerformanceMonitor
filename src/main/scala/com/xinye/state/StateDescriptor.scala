package com.xinye.state

import com.xinye.base.Rule
import com.xinye.enums.impl.RuleStateEnum
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time

import java.util.Set
import java.util.Map
import scala.collection.mutable.ArrayBuffer

object StateDescriptor {

  val ttlConfig: StateTtlConfig = StateTtlConfig.newBuilder(Time.hours(2))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .cleanupFullSnapshot()
    .build()

  val ruleState = new MapStateDescriptor[String, Rule]("RuleState", classOf[String], classOf[Rule])

  val detailState = new MapStateDescriptor[String, Map[Long, ArrayBuffer[Map[String, String]]]]("detailState", classOf[String], classOf[Map[Long, ArrayBuffer[Map[String, String]]]])

  detailState.enableTimeToLive(ttlConfig)

  val aggState = new MapStateDescriptor("aggState", classOf[String], classOf[Map[Long, Map[String, String]]])

  aggState.enableTimeToLive(ttlConfig)

  val hostNote = new MapStateDescriptor("hostNote", classOf[String], classOf[Int])

  hostNote.enableTimeToLive(ttlConfig)

  val timeHostNote = new MapStateDescriptor("timeHostNote", classOf[Long], classOf[Set[String]])

  def changeBroadcastState(value: Rule, ruleState: BroadcastState[String, Rule]): Boolean = {
    if (value != null && ruleState != null) {
      RuleStateEnum.fromString(value.getRuleState) match {
        case RuleStateEnum.START => ruleState.put(value.getRuleID, value)
        case RuleStateEnum.DELETE => ruleState.remove(value.getRuleID)
        case RuleStateEnum.STOP =>
          if (ruleState.contains(value.getRuleID)) {
            val rule = ruleState.get(value.getRuleID)
            if (rule != null) {
              rule.setRuleState(RuleStateEnum.STOP.toString)
            }
            ruleState.put(rule.getRuleID, rule)
          }
      }
      true
    } else {
      false
    }
  }

  val customs = new MapStateDescriptor[String, Map[Map[String, String], Int]]("customs", classOf[String], classOf[Map[Map[String, String], Int]])

}