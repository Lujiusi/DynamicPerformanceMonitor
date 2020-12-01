package com.xinye.operator

import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.enums.RuleSateEnum
import com.xinye.operator.pojo.DynamicKey
import org.apache.flink.api.common.state.ReadOnlyBroadcastState
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import java.util.Map

import com.alibaba.fastjson.JSONObject

import scala.collection.JavaConversions._

class DynamicKeyedMapFunction extends BroadcastProcessFunction[java.util.Map[String, String], Rule, (DynamicKey, Map[String, String])] {

  override def processElement(value: java.util.Map[String, String],
                              ctx: BroadcastProcessFunction[java.util.Map[String, String], Rule, (DynamicKey, Map[String, String])]#ReadOnlyContext,
                              out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    // 按照 分组字段进行分组
    val ruleState: ReadOnlyBroadcastState[Integer, Rule] = ctx.getBroadcastState(StateDescriptor.dynamicKeyedMapState)
    setKeyForEventMetrics(value, ruleState, out)
  }

  override def processBroadcastElement(value: Rule,
                                       ctx: BroadcastProcessFunction[java.util.Map[String, String], Rule, (DynamicKey, Map[String, String])]#Context,
                                       out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    StateDescriptor.changeBroadcastState(value, ctx.getBroadcastState(StateDescriptor.dynamicKeyedMapState))
  }

  override def close(): Unit = super.close()

  def setKeyForEventMetrics(metrics: java.util.Map[String, String],
                            states: ReadOnlyBroadcastState[Integer, Rule],
                            out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    // 遍历状态中所有的 rule
    states.immutableEntries().iterator()
      .foreach { entry => {
        val rule = entry.getValue
        RuleSateEnum.fromString(rule.getRuleState) match {
          case RuleSateEnum.START =>
            // appName 为空表示所有 或者 appName 包含当前AppName 且满足 字符串的字段
            if (filter(metrics.get("appName"), rule.getAppName)
              && filter(metrics.get("env"), rule.getEnv)
              && filter(metrics, rule.getFilters)
              && "agent.heartbeat".equals(metrics.get("datasource"))) {
              val keyJson = new JSONObject()
              keyJson.put("domain", metrics.get("appName"))
              //              keyJson.put("datasource", metrics.get("datasource"))
              out.collect((DynamicKey(rule.getRuleID, keyJson.toJSONString, 0), metrics))
            }
          case _ =>
        }
      }
      }
  }

  def filter(value: java.util.Map[String, String], filterMap: java.util.Map[String, String]): Boolean = {
    if (filterMap.size() == 0) {
      true
    } else {
      filterMap
        .forall(entry => {
          "*".equals(entry._2) || entry._2.split(",").contains(value.get(entry._1))
        })
    }
  }

  def filter(value: String, range: java.util.List[String]): Boolean = {
    range.size() == 0 || range.contains(value) || "*".equals(range.head)
  }

}

object DynamicKeyedMapFunction {

  private val log: Logger = LoggerFactory.getLogger(classOf[DynamicKeyedMapFunction])

  /*
    参数1代表取数字段名
    参数2代表每条数据
 */
  def getGroupKey(str: String, metrics: java.util.Map[String, String]): Option[Any] = {
    var result = Option[Any](null)
    if (str != null) {
      if (metrics.containsKey(str)) {
        result = Option(metrics.get(str))
      } else {
        log.error("{}该key可能存在定位可能出现错误请重新编写规则！", str)
      }
    }
    result
  }

}
