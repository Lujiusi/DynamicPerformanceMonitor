package com.xinye.operator

import com.xinye.base.Rule.AggregatorFun
import com.xinye.enums.ComputeEnum

import scala.collection.JavaConversions._
import java.util.Map

import com.alibaba.fastjson

import scala.collection.mutable

object AggregationFunction {

  def sumByRule(currentRule: AggregatorFun,
                metricByKeyState: List[Map[String, String]]): List[mutable.Map[String, String]] = {
    // 获取分组字段
    val groupingKeyNames = currentRule.getGroupingKeyNames

    //获取别名
    val aliasName = currentRule.getAliasName

    // 获取所需计算字段
    val column = currentRule.getComputeColumn

    //如果过滤条件 不为空 则进行过滤操作
    val filterList = metricsFilter(currentRule.getFilters, metricByKeyState)

    val list = if (groupingKeyNames.size() == 0) {
      // 如果不需要分组
      val sum = filterList.map(m => {
        val option = DynamicKeyedMapFunction.getUniqueKey(column, m)
        if (option.isDefined) option.get.toString.toDouble else 0
      }).sum
      // (别名 -> 统计结果)
      List(mutable.Map(aliasName -> sum.toString))
    } else {
      val tuplesToDouble = filterList.groupBy(metric => groupingKeyNames.map(c => DynamicKeyedMapFunction.getUniqueKey(c, metric).get.toString))
        .map { tuple =>
          (groupingKeyNames.zip(tuple._1), tuple._2.map(metric => {
            val option = DynamicKeyedMapFunction.getUniqueKey(column, metric)
            if (option.isDefined) option.get.toString.toDouble else 0
          }).sum.toString)
        }
      // (   (分组1 -> 分组1结果,分组2 -> 分组2结果 , ... , 别名 - > 分组聚合结果) ... )
      listToMap(tuplesToDouble, aliasName)
    }
    list
  }

  def countByRule(currentRule: AggregatorFun,
                  metricByKeyState: List[Map[String, String]]): List[mutable.Map[String, String]] = {

    val groupingKeyNames = currentRule.getGroupingKeyNames

    val aliasName = currentRule.getAliasName

    val column = currentRule.getComputeColumn

    val distinctColumns = currentRule.getDistinctColumns

    val filterList = metricsFilter(currentRule.getFilters, metricByKeyState)

    val list = if (groupingKeyNames.size() == 0) {
      val size = if (distinctColumns == null || distinctColumns.isEmpty) {
        filterList.size
      } else {
        filterList.map(metric => distinctColumns.map(col => DynamicKeyedMapFunction.getUniqueKey(col, metric))).distinct.size
      }
      List(mutable.Map(aliasName -> size.toString))
    } else {
      // 对数据进行分组
      val groups = filterList.groupBy(metric => groupingKeyNames.map(c => DynamicKeyedMapFunction.getUniqueKey(c, metric).get.toString))
        .map { tuple =>
          (groupingKeyNames.zip(tuple._1), tuple._2.map(m => DynamicKeyedMapFunction.getUniqueKey(column, m).get.toString))
        }
      val tuplesToDouble = if (distinctColumns.isEmpty || distinctColumns == null) {
        groups.mapValues(_.size.toString)
      } else {
        groups.mapValues(_.distinct.size.toString)
      }
      listToMap(tuplesToDouble, aliasName)
    }

    list
  }

  /**
   * 过滤数据
   *
   * @param filters          过滤规则
   * @param metricByKeyState 数据集合
   * @return
   */
  def metricsFilter(filters: fastjson.JSONObject, metricByKeyState: List[Map[String, String]]): List[Map[String, String]] = {
    if (filters != null && filters.size() != 0) {
      metricByKeyState.filter(metric => DynamicFilterFunction.filter(metric, filters))
    } else {
      metricByKeyState
    }
  }

  //
  //  def countDistinctByRule(currentRule: AggregatorFun, metricByKeyState: List[Map[String, Object]]): List[Map[String, Any]] = {
  //    val distinct: List[Map[String, Object]] = metricByKeyState.distinct
  //    val countDistinct = countByRule(currentRule, distinct)
  //    countDistinct
  //  }
  //
  //  def sumDistinctByRule(currentRule: AggregatorFun, metricByKeyState: List[Map[String, Object]]): List[scala.collection.Map[String, Any]] = {
  //    val distinct: List[Map[String, Object]] = metricByKeyState.distinct
  //    val sumDistinctArr = sumByRule(currentRule, distinct)
  //    sumDistinctArr
  //  }
  //
  //
  //  //
  //  def computeByStep(currentRule: AggregatorFun, metricByKeyState: List[Map[String, Object]]): List[(Map[String, Any], Map[String, Object])] = {
  //
  //    val filters = currentRule.getFilters
  //    //根据给定的filterList进行过滤操作
  //    var filterList: List[Map[String, Object]] = null
  //
  //    // 如果是 含有过滤条件 则 及进行过滤操作 否则直接赋值
  //    if (filters != null && filters.size() != 0) {
  //      filterList = metricByKeyState.filter(metric => DynamicFilterFuntion.filter(metric, filters))
  //    } else {
  //      filterList = metricByKeyState
  //    }
  //
  //    //去重处理，去掉完全重复的数据
  //    val distinctList: List[Map[String, Object]] = filterList.distinct
  //
  //    val aliasName = currentRule.getAliasName
  //    val column = currentRule.getComputeColumn
  //    val startStep = currentRule.getStartStep
  //    val endStep = currentRule.getEndStep
  //    val stepColumn = currentRule.getStepColumn
  //    val operator = currentRule.getOperator
  //    val groupName = currentRule.getGroupingKeyNames
  //
  //    // 率先封装 结果
  //    var result: (scala.collection.mutable.Map[String, Any], scala.collection.mutable.Map[String, Object]) = null
  //
  //    var list: List[(scala.collection.mutable.Map[String, Any], scala.collection.mutable.Map[String, Object])] = null
  //
  //    // 如果分组字段为空 (说明不需要分组)
  //    if (groupName.isEmpty) {
  //
  //      // 选出起始数据 list
  //      val startMetrics = distinctList.filter(metric => startStep.equalsIgnoreCase(DynamicKeyedMapFunction.getUniqueKey(stepColumn, metric).get.toString))
  //        .sortBy(metric => DynamicKeyedMapFunction.getUniqueKey(column, metric).get.toString.toDouble)
  //
  //      // 选出结束数据 list
  //      val endMetrics = distinctList.filter(metric => endStep.equalsIgnoreCase(DynamicKeyedMapFunction.getUniqueKey(stepColumn, metric).get.toString))
  //        .sortBy(metric => DynamicKeyedMapFunction.getUniqueKey(column, metric).get.toString.toDouble)
  //
  //      if (startMetrics.nonEmpty && endMetrics.nonEmpty) {
  //
  //        val startMetric = startMetrics.head
  //        val endMetric = endMetrics.head
  //
  //        val start = DynamicKeyedMapFunction.getUniqueKey(column, startMetric).get.toString.toDouble
  //        val end = DynamicKeyedMapFunction.getUniqueKey(column, endMetric).get.toString.toDouble
  //
  //        val tag = ComputeEnum.fromString(operator) match {
  //          case ComputeEnum.addition => end + start
  //          case ComputeEnum.division => end / start
  //          case ComputeEnum.subtraction => end - start
  //          case ComputeEnum.multiplication => end * start
  //        }
  //
  //        endMetric.put(aliasName, tag.asInstanceOf[Object])
  //        result = (mutable.Map(aliasName -> tag), endMetric)
  //
  //      } else if (startMetrics.nonEmpty && endMetrics.isEmpty) {
  //
  //        // 如果只有开始  , 没有结束 则 给予默认值 -1.0
  //        val startMetric = startMetrics.head
  //
  //        startMetric.put(aliasName, Double.box(-1.0))
  //
  //        result = (mutable.Map(aliasName -> Double.box(-1.0)), startMetric)
  //
  //      }
  //      list = List(result)
  //    } else {
  //
  //      // 如果分组字段不为空 (说明需要分组)  在这一步进行分组
  //      val groupMetrics = distinctList.groupBy(metric => groupName.map(c => DynamicKeyedMapFunction.getUniqueKey(c, metric).get.toString))
  //
  //      //分组名和分组数据 做 zip 映射到 分组结果 上
  //      val everyStepResult: Predef.Map[mutable.Buffer[(String, String)], (Double, mutable.Map[String, Object])] = groupMetrics.map(tuple => (groupName.zip(tuple._1), tuple._2))
  //
  //        .mapValues(values => {
  //
  //          var result: (Double, mutable.Map[String, Object]) = null
  //
  //          val startMetrics = values.filter(metric => startStep.equalsIgnoreCase(DynamicKeyedMapFunction.getUniqueKey(stepColumn, metric).get.toString))
  //            .sortBy(metric => DynamicKeyedMapFunction.getUniqueKey(column, metric).get.toString.toDouble)
  //
  //          val endMetric = values.filter(metric => endStep.equalsIgnoreCase(DynamicKeyedMapFunction.getUniqueKey(stepColumn, metric).get.toString))
  //
  //          if (startMetrics.nonEmpty) {
  //
  //            val startMetric: mutable.Map[String, Object] = startMetrics.head
  //
  //            if (endMetric.isEmpty || startMetrics.isEmpty) {
  //
  //              startMetric.put(aliasName, Double.box(-1.0))
  //
  //            } else {
  //
  //              val start = DynamicKeyedMapFunction.getUniqueKey(column, startMetrics.head).get.toString.toDouble
  //              val end = DynamicKeyedMapFunction.getUniqueKey(column, endMetric.head).get.toString.toDouble
  //
  //              val tag = ComputeEnum.fromString(operator) match {
  //                case ComputeEnum.addition => end + start
  //                case ComputeEnum.division => end / start
  //                case ComputeEnum.subtraction => end - start
  //                case ComputeEnum.multiplication => end * start
  //              }
  //
  //              startMetric.put(aliasName, Double.box(tag))
  //
  //              result = (tag, startMetric)
  //
  //            }
  //          }
  //
  //          result
  //
  //        }).filter(tuple => tuple._2 != null) //过滤掉 result 为 null的 数据也就是 开始数据为空的数据
  //
  //      list = listTupleValueToMap(everyStepResult, aliasName)
  //
  //    }
  //    list
  //  }
  //
  //  def listTupleValueToMap(tuplesToDouble: scala.collection.Map[mutable.Buffer[(String, String)], (Double, mutable.Map[String, Object])], aliasName: String): List[(Map[String, Any], Map[String, Object])] = {
  //    val result = tuplesToDouble.map(tuple => {
  //
  //      //取出 Map
  //      val map = Map[String, Any](aliasName -> tuple._2._1)
  //
  //      //将聚合出来的指标完全 加入到 map 当中
  //      for (t <- tuple._1) {
  //        map.put(t._1, t._2)
  //      }
  //
  //      (map, tuple._2._2)
  //
  //    }).toList
  //
  //    result
  //  }
  //

  def listToMap(tuplesToDouble: scala.collection.Map[mutable.Buffer[(String, String)], String],
                aliasName: String): List[mutable.Map[String, String]] = {
    // 将数据写成 map 形式
    val result: List[mutable.Map[String, String]] = tuplesToDouble.map(tuple => {
      val map = mutable.Map[String, String](aliasName -> tuple._2)
      for (t <- tuple._1) {
        map.put(t._1, t._2)
      }
      map
    }).toList
    result
  }
}

