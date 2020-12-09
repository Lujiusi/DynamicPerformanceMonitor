package com.xinye.operator

import com.alibaba.fastjson.JSONObject
import com.xinye.enums.impl.{LimitOperatorEnum, LogicEnum}

import scala.collection.JavaConversions._
import java.util.Map

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
      case _ =>
        compareValue(metrics.get(filters.getString("key")),
          filters.getString("value"),
          filters.getString("operator"))
    }
  }

  def filter(value: Map[String, String], filters: Map[String, String]): Boolean = {
    if (filters != null) {
      filters.forall(entry => {
        entry._2.split(",").contains(value.get(entry._1))
      })
    } else true

  }

  def compareValue(value: String, target: String, operator: String): Boolean = {
    LimitOperatorEnum.fromString(operator) match {
      case LimitOperatorEnum.EQUAL => value.equalsIgnoreCase(target)
      case LimitOperatorEnum.NOT_EQUAL => !value.equalsIgnoreCase(target)
      case LimitOperatorEnum.LESS => value.toDouble > target.toDouble
      case LimitOperatorEnum.LESS_EQUAL => value.toDouble >= target.toDouble
      case LimitOperatorEnum.GREATER => value.toDouble < target.toDouble
      case LimitOperatorEnum.GREATER_EQUAL => value.toDouble <= target.toDouble
      case LimitOperatorEnum.IN => target.split(",").contains(value)
      case LimitOperatorEnum.NOTIN => !target.split(",").contains(value)
      case LimitOperatorEnum.REGEX => target.r.findFirstIn(value).isDefined
    }
  }

}