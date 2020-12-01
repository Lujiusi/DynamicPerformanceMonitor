package com.xinye.operator

import com.xinye.base.Rule.AggregatorFun
import java.util.Map

import com.xinye.enums.AggregatorFunctionType
import scala.collection.JavaConversions._
import com.alibaba.fastjson.JSONObject


object AggregationFunction {

  def calculate(fun: AggregatorFun,
                metricList: List[Map[String, String]]): java.util.Map[String, String] = {
    val filterMetrics = metricsFilter(fun.getFilter, metricList)
    metricList.groupBy(metric => {
      getGroupKey(fun.getGroupingNames, metric)
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
   * 过滤数据
   *
   * @param filters          过滤规则
   * @param metricByKeyState 数据集合
   * @return
   */
  def metricsFilter(filters: JSONObject, metricByKeyState: List[Map[String, String]]): List[Map[String, String]] = {
    if (filters != null && filters.size() != 0) {
      metricByKeyState.filter(metric => DynamicFilterFunction.filter(metric, filters))
    } else {
      metricByKeyState
    }
  }


  def getGroupKey(groupNames: java.util.List[String], metric: java.util.Map[String, String]): String = {
    var result = ""
    if (groupNames != null) {
      val jsonKey = new JSONObject()
      groupNames.foreach(name => {
        jsonKey.put(name, metric.get(name))
      })
      jsonKey.put("datasource", metric.get("datasource"))
      result = result + jsonKey.toJSONString
    }
    result
  }

}

