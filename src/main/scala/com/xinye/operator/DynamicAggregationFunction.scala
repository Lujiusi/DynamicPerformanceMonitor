package com.xinye.operator

import java.util

import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.enums.RuleSateEnum
import com.xinye.operator.pojo.DynamicKey
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.util.Map

import com.alibaba.fastjson.JSONObject

class DynamicAggregationFunction extends KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, (DynamicKey, Map[String, String])] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DynamicAggregationFunction])

  // 用于保存状态数据
  lazy val metricByKeyState: MapState[String, Map[Long, ArrayBuffer[Map[String, String]]]] = getRuntimeContext.getMapState(StateDescriptor.metricByKeyState)

  // 时间 -> 别名 -> 分组 -> 数据
  lazy val timeToAliasName: MapState[Long, Map[String, Map[String, String]]] = getRuntimeContext.getMapState(StateDescriptor.timeToAliasName)

  override def processElement(value: (DynamicKey, Map[String, String]),
                              ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, (DynamicKey, Map[String, String])]#ReadOnlyContext,
                              collector: Collector[(DynamicKey, Map[String, String])]): Unit = {

    val aggState: ReadOnlyBroadcastState[Integer, Rule] = ctx.getBroadcastState(StateDescriptor.dynamicAggregateRuleMapState)
    val ruleId: Int = value._1.id
    if (aggState.contains(ruleId)) {
      RuleSateEnum.fromString(aggState.get(ruleId).getRuleState) match {
        case RuleSateEnum.START =>
          //将数据加入到对应 datasource 和 时间 内
          val dataSource = value._2.get("datasource")
          val timeStamp: Long = value._2.get("timestamp").toLong / (1 * 60 * 1000) * 1 * 60 * 1000
          if (metricByKeyState.get(dataSource) == null) {
            metricByKeyState.put(dataSource, new util.HashMap[Long, ArrayBuffer[util.Map[String, String]]]())
          }
          if (metricByKeyState.get(dataSource).get(timeStamp) == null) {
            metricByKeyState.get(dataSource).put(timeStamp, new ArrayBuffer[Map[String, String]]())
          }
          metricByKeyState.get(dataSource).get(timeStamp).append(value._2)

          // 注册当前时间下一分钟的开始时间的定时器
          ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() / (1 * 60 * 1000) * 1 * 60 * 1000 + 1 * 60 * 1000)
          println("定时器注册成功")
        case _ => println("没匹配上")
      }
    } else {
      logger.error("Rule with ID {} does not exist", ruleId)
    }

  }

  override def processBroadcastElement(rule: Rule,
                                       ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, (DynamicKey, Map[String, String])]#Context,
                                       collector: Collector[(DynamicKey, Map[String, String])]): Unit = {
    StateDescriptor.changeBroadcastState(rule, ctx.getBroadcastState(StateDescriptor.dynamicAggregateRuleMapState))
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, (DynamicKey, Map[String, String])]#OnTimerContext,
                       out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    val rule = ctx.getBroadcastState(StateDescriptor.dynamicAggregateRuleMapState).get(ctx.getCurrentKey.id)

    if (ruleIsAvailable(rule)) {

      val funList = rule.getAggregatorFun

      val allDataSource = funList.map(_.getDatasource).distinct

      // 选出 不在 此 rule 统计中的 datasource 进行清除
      metricByKeyState.keys()
        .filter(key => !allDataSource.contains(key))
        .foreach(metricByKeyState.remove)

      // 已经超时的数据
      funList.groupBy(_.getDatasource)
        .mapValues(iter => iter.map(_.getWindow).max)
        .foreach(entry => {
          val metricMap = metricByKeyState.get(entry._1)
          metricMap.map(_._1)
            .filter(_ < timestamp - entry._2 * 60 * 1000)
            .foreach(metricMap.remove)
        })

      val time = timestamp - 1 * 60 * 1000

      // 初始化 结果存储对象
      if (timeToAliasName.get(time) == null) {
        timeToAliasName.put(time, new util.HashMap[String, Map[String, String]]())
      }

      val resultMap = new util.HashMap[String, Map[String, String]]()
      funList.foreach(fun => {
        val window = fun.getWindow
        val datasource = fun.getDatasource
        val metricMap = metricByKeyState.get(datasource)
        val metricList = metricMap.filter(_._1 > timestamp - window * 60 * 1000).flatMap(_._2)

        if (metricList.nonEmpty) {
          val calculateResult = AggregationFunction.calculate(fun, metricList.toList)
          println("-----" + calculateResult)
        }

      })
    } else {
      metricByKeyState.clear()
    }

  }

  def ruleIsAvailable(rule: Rule): Boolean = {
    var tag: Boolean = false
    if (rule != null) {
      tag = RuleSateEnum.fromString(rule.getRuleState) match {
        case RuleSateEnum.START => true
        case _ => false
      }
    }
    tag
  }

  def postCalculate(metricList: Map[String, Map[String, String]], calFun: JSONObject): Unit = {
    val fields = calFun.getJSONArray("fields")
    val operator = calFun.getString("operator")
    val first_fields = fields.get(0)
  }

}