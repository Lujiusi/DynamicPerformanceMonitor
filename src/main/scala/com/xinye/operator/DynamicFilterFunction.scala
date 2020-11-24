package com.xinye.operator

import com.alibaba.fastjson.JSONObject
import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.enums.{LimitOperatorEnum, LogicEnum, RuleSateEnum}
import com.xinye.operator.pojo.DynamicKey
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import java.util.Map

import org.apache.flink.api.common.state.ReadOnlyBroadcastState

class DynamicFilterFunction extends BroadcastProcessFunction[(DynamicKey, Map[String, String]), Rule, (DynamicKey, Map[String, String])] {

  override def processElement(value: (DynamicKey, Map[String, String]), ctx: BroadcastProcessFunction[(DynamicKey, Map[String, String]), Rule, (DynamicKey, Map[String, String])]#ReadOnlyContext, out: Collector[(DynamicKey, Map[String, String])]): Unit = {

    val ruleState: ReadOnlyBroadcastState[Integer, Rule] = ctx.getBroadcastState(StateDescriptor.dynamicFilterRuleMapState)

    val ruleID: Int = value._1.id

    //找到 本条 数据中 ruleID 所对应的 rule
    if (ruleState.contains(ruleID)) {
      val rule: Rule = ruleState.get(ruleID)
      RuleSateEnum.fromString(rule.getRuleState) match {
        case RuleSateEnum.ACTIVE =>
          if (DynamicFilterFunction.filter(value._2, rule.getFilters)) out.collect(value)
        case _ =>
      }
    }

  }

  override def processBroadcastElement(value: Rule,
                                       ctx: BroadcastProcessFunction[(DynamicKey, Map[String, String]), Rule, (DynamicKey, Map[String, String])]#Context,
                                       out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    StateDescriptor.changeBroadcastState(value, ctx.getBroadcastState(StateDescriptor.dynamicFilterRuleMapState))
  }

}

object DynamicFilterFunction {

  def filter(metrics: Map[String, String], filters: JSONObject): Boolean = {
    LogicEnum.fromString(filters.getString("type")) match {
      // 遇到 判断连接条件为 and ,则 内部所有判断条件都为 true
      case LogicEnum.AND =>
        filters.getJSONArray("fields").toArray.forall(bool => filter(metrics, bool.asInstanceOf[JSONObject]))
      // 遇到 判断连接条件为or , 则 内部只要存在一个 true 就行
      case LogicEnum.OR =>
        filters.getJSONArray("fields").toArray.exists(bool => filter(metrics, bool.asInstanceOf[JSONObject]))
      // 连接条件为 selector 则 正常判断
      case LogicEnum.SELECTOR =>
        val key: String = filters.getString("key")
        val operator: String = filters.getString("operator")
        val value: String = filters.getString("value")
        val metricTag: String = DynamicKeyedMapFunction.getUniqueKey(key, metrics).get.toString
        LimitOperatorEnum.fromString(operator) match {
          case LimitOperatorEnum.EQUAL => value.equalsIgnoreCase(metricTag)
          case LimitOperatorEnum.NOT_EQUAL => !value.equalsIgnoreCase(metricTag)
          case LimitOperatorEnum.LESS => value.toDouble > metricTag.toDouble
          case LimitOperatorEnum.LESS_EQUAL => value.toDouble >= metricTag.toDouble
          case LimitOperatorEnum.GREATER => value.toDouble < metricTag.toDouble
          case LimitOperatorEnum.GREATER_EQUAL => value.toDouble <= metricTag.toDouble
          case LimitOperatorEnum.IN => metricTag.split(",").contains(value)
          case LimitOperatorEnum.NOTIN => !metricTag.split(",").contains(value)
          case LimitOperatorEnum.REGEX => metricTag.r.findFirstIn(value).isDefined
        }
    }
  }

}