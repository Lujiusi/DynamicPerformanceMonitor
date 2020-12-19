package com.xinye.operator

import com.alibaba.fastjson.JSONObject
import com.xinye.base.Rule
import com.xinye.base.Rule.AggregatorFun
import com.xinye.enums.impl.{AggregatorFunctionType, LimitOperatorEnum, LogicEnum, RuleStateEnum}

import scala.collection.JavaConversions._
import java.util.Map

/**
 * 一些公共方法
 *
 * @author daiwei04@xinye.com
 * @since 2020/12/11 22:18
 */
object CommonFunction {

  /**
   * 将选出的数据集合 按照分组 进行计算
   *
   * @param fun        计算函数
   * @param metricList 数据集合
   * @return
   */
  def calculate(fun: AggregatorFun,
                metricList: List[Map[String, String]]): Map[String, String] = {
    var filterMetrics = metricList
    val filters = fun.getFilters
    if (filters != null && filters.size() != 0) {
      filterMetrics = metricList.filter(metric => filter(metric, filters))
    }
    filterMetrics.groupBy(metric => {
      val groupingNames = fun.getGroupingNames
      val jsonKey = new JSONObject()
      if (groupingNames != null) {
        groupingNames.foreach(name => {
          jsonKey.put(name, metric.get(name))
        })
      }
      jsonKey.toJSONString
    }).mapValues {
      metricIter => {
        val resultValue = AggregatorFunctionType.fromString(fun.getAggregatorFunctionType) match {
          case AggregatorFunctionType.SUM =>
            metricIter.map(_.get(fun.getComputeColumn).toDouble).sum
          case AggregatorFunctionType.COUNT =>
            metricIter.size
          case AggregatorFunctionType.AVG =>
            metricIter.map(_.get(fun.getComputeColumn).toDouble).sum / metricIter.size
          case AggregatorFunctionType.MAX =>
            metricIter.map(_.get(fun.getComputeColumn).toDouble).max
          case AggregatorFunctionType.MIN =>
            metricIter.map(_.get(fun.getComputeColumn).toDouble).min
        }
        resultValue.toString
      }
    }
  }

  /**
   * 执行 aggregateFunction 的过滤条件
   *
   * @param value   一条指标信息
   * @param filters 过滤 json
   * @return
   */
  def filter(value: Map[String, String], filters: JSONObject): Boolean = {
    LogicEnum.fromString(filters.getString("type")) match {
      // 遇到 判断连接条件为 and ,则 内部所有判断条件都为 true
      case LogicEnum.AND =>
        filters.getJSONArray("fields").toArray.forall(bool => filter(value, bool.asInstanceOf[JSONObject]))
      // 遇到 判断连接条件为or , 则 内部只要存在一个 true 就行
      case LogicEnum.OR =>
        filters.getJSONArray("fields").toArray.exists(bool => filter(value, bool.asInstanceOf[JSONObject]))
      // 连接条件为 selector 则 正常判断
      case _ =>
        compareValue(value.get(filters.getString("key")),
          filters.getString("value"),
          filters.getString("operator"))
    }
  }

  /**
   * 数据比较
   *
   * @param value    当前值
   * @param target   目标值
   * @param operator 比较符
   * @return
   */
  def compareValue(value: String, target: String, operator: String): Boolean = {
    LimitOperatorEnum.fromString(operator) match {
      case LimitOperatorEnum.EQUAL => value.equalsIgnoreCase(target)
      case LimitOperatorEnum.NOT_EQUAL => !value.equalsIgnoreCase(target)
      case LimitOperatorEnum.LESS => value.toDouble > target.toDouble
      case LimitOperatorEnum.LESS_EQUAL => value.toDouble >= target.toDouble
      case LimitOperatorEnum.GREATER => value.toDouble < target.toDouble
      case LimitOperatorEnum.GREATER_EQUAL => value.toDouble <= target.toDouble
      case LimitOperatorEnum.IN => target.split(",").contains(value)
      case _ => println(value + " : " + target + " : " + operator)
        false
    }
  }

  /**
   * 进行第一个 filter 的过滤
   *
   * @param value   一条数据
   * @param filters 过滤条件  example : [appName:"1.1.1.1,2.2.2.2"...]
   * @return
   */
  def filter(value: Map[String, String], filters: Map[String, String]): Boolean = {
    if (filters != null) {
      filters.forall(entry => {
        entry._2.split(",").contains(value.get(entry._1))
      })
    } else true
  }

  /**
   * 判断规则的状态是否为 start (激活态)
   *
   * @param rule 当前规则
   * @return
   */
  def ruleIsAvailable(rule: Rule): Boolean = {
    var tag: Boolean = false
    if (rule != null) {
      tag = RuleStateEnum.fromString(rule.getRuleState) match {
        case RuleStateEnum.START => true
        case _ => false
      }
    }
    tag
  }

}
