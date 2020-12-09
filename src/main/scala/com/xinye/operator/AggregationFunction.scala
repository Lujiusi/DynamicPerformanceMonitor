package com.xinye.operator

import com.xinye.base.Rule.AggregatorFun

import java.util.Map
import scala.collection.JavaConversions._
import com.alibaba.fastjson.JSONObject
import com.xinye.enums.impl.AggregatorFunctionType


object AggregationFunction {

  def calculate(fun: AggregatorFun,
                metricList: List[Map[String, String]]): java.util.Map[String, String] = {
    var filterMetrics = metricList
    val filters = fun.getFilters
    //    println("filterMetrics 1 " + filterMetrics)
    if (filters != null && filters.size() != 0) {
      filterMetrics = metricList.filter(metric => DynamicFilterFunction.filter(metric, filters))
    }
    println("filterMetrics 2 " + filterMetrics)
    filterMetrics.groupBy(metric => {
      val groupingNames = fun.getGroupingNames
      val jsonKey = new JSONObject()
      if (groupingNames != null) {
        groupingNames.foreach(name => {
          jsonKey.put(name, metric.get(name))
        })
      }
      jsonKey.put("datasource", metric.get("datasource"))
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

}

