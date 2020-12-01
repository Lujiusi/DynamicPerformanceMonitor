package com.xinye.operator

import java.util

import com.xinye.base.{Alarm, Rule}
import com.xinye.operator.pojo.DynamicKey
import com.xinye.config.state.StateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import java.util.Map


class DynamicPostAggregateFun extends
  KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, Alarm] {

  //  lazy val timeToAlias = getRuntimeContext.getMapState[Long, Map[String, String]](StateDescriptor.timeToAliasName)

  override def processElement(value: (DynamicKey, Map[String, String]),
                              readOnlyContext: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, Alarm]#ReadOnlyContext,
                              collector: Collector[Alarm]): Unit = {
    val timestamp = value._1.timestamp
    //    if (timeToAlias.get(timestamp) == null) {
    //      timeToAlias.put(timestamp, new util.HashMap[String, String]())
    //    }
    //    timeToAlias.put("")
  }

  override def processBroadcastElement(rule: Rule,
                                       ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, Alarm]#Context,
                                       collector: Collector[Alarm]): Unit = {
    StateDescriptor.changeBroadcastState(rule, ctx.getBroadcastState(StateDescriptor.dynamicPostAggregateRuleMapState))
  }

  //  def postCalculate(metricList: Map[String, Map[String, String]],): Unit = {
  //
  //  }

}


