package com.xinye.operator

import com.xinye.base.Rule
import com.xinye.pojo.DynamicKey
import com.xinye.state.StateDescriptor
import com.alibaba.fastjson.JSONObject
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

import java.util.{HashMap, Map}
import scala.collection.JavaConversions._

class DynamicKeyedMapFunction extends BroadcastProcessFunction[Map[String, String], Rule, (DynamicKey, Map[String, String])] {

  override def processElement(value: Map[String, String],
                              ctx: BroadcastProcessFunction[Map[String, String], Rule, (DynamicKey, Map[String, String])]#ReadOnlyContext,
                              out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    if (value.size() != 0) {
      ctx.getBroadcastState(StateDescriptor.ruleState)
        .immutableEntries()
        .foreach(entry => {
          val rule = entry.getValue
          if (CommonFunction.ruleIsAvailable(rule)) {
            // appName 为空表示所有 或者 appName 包含当前AppName 且满足 字符串的字段
            if (CommonFunction.filter(value, rule.getFilters)
              //              && rule.getAggregatorFun.map(_.getDatasource).contains(value.get("datasource"))
              && value.get("timestamp").toLong < System.currentTimeMillis() + 1000 * 60 * 12) {
              val key = new JSONObject()
              key.put("appName", value.get("appName"))
              key.put("env", value.get("env"))
              val result = new HashMap[String, String](value)
              // 移除datasource,算是减少状态大小
              result.remove("appName")
              result.remove("env")
              out.collect((DynamicKey(rule.getRuleID, key.toJSONString), result))
            }
          }
        })
    }
  }

  override def processBroadcastElement(value: Rule,
                                       ctx: BroadcastProcessFunction[Map[String, String], Rule, (DynamicKey, Map[String, String])]#Context,
                                       out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    StateDescriptor.changeBroadcastState(value, ctx.getBroadcastState(StateDescriptor.ruleState))
  }

}