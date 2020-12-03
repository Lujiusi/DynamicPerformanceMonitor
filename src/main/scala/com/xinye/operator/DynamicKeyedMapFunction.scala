package com.xinye.operator

import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.pojo.DynamicKey
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

import java.util.Map
import com.alibaba.fastjson.JSONObject
import com.xinye.enums.impl.RuleSateEnum

import scala.collection.JavaConversions._

class DynamicKeyedMapFunction extends BroadcastProcessFunction[java.util.Map[String, String], Rule, (DynamicKey, Map[String, String])] {

  override def processElement(value: java.util.Map[String, String],
                              ctx: BroadcastProcessFunction[java.util.Map[String, String], Rule, (DynamicKey, Map[String, String])]#ReadOnlyContext,
                              out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    // 按照 分组字段进行分组
    ctx.getBroadcastState(StateDescriptor.dynamicKeyedMapState)
      .immutableEntries()
      .iterator()
      .foreach { entry => {
        val rule = entry.getValue
        RuleSateEnum.fromString(rule.getRuleState) match {
          case RuleSateEnum.START =>
            // appName 为空表示所有 或者 appName 包含当前AppName 且满足 字符串的字段
            if (DynamicFilterFunction.filter(value.get("appName"), rule.getAppName)
              && DynamicFilterFunction.filter(value.get("env"), rule.getEnv)
              && DynamicFilterFunction.filter(value, rule.getFilters)) {
              val key = new JSONObject()
              key.put("datasource", value.get("datasource"))
              out.collect((DynamicKey(rule.getRuleID, key.toJSONString, 0), value))
            }
          case _ =>
        }
      }
      }
  }

  override def processBroadcastElement(value: Rule,
                                       ctx: BroadcastProcessFunction[Map[String, String], Rule, (DynamicKey, Map[String, String])]#Context,
                                       out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    StateDescriptor.changeBroadcastState(value, ctx.getBroadcastState(StateDescriptor.dynamicKeyedMapState))
  }


}