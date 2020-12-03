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
      case LogicEnum.SELECTOR =>
        val key: String = filters.getString("key")
        val operator: String = filters.getString("operator")
        val value: String = filters.getString("value")
        val metricTag: String = metrics.get(key)
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