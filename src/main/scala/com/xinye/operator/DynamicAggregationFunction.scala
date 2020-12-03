package com.xinye.operator

import java.text.DecimalFormat
import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.pojo.DynamicKey
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.util.Map
import java.util.HashMap
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import com.xinye.enums.impl.{ComputeEnum, RuleSateEnum}

class DynamicAggregationFunction extends KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, (DynamicKey, Map[String, String])] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DynamicAggregationFunction])

  lazy val detailState: MapState[Long, ArrayBuffer[Map[String, String]]] = getRuntimeContext.getMapState(StateDescriptor.detailState)

  lazy val aggState: MapState[String, Map[Long, Map[String, String]]] = getRuntimeContext.getMapState(StateDescriptor.aggState)

  override def processElement(value: (DynamicKey, Map[String, String]),
                              ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, (DynamicKey, Map[String, String])]#ReadOnlyContext,
                              collector: Collector[(DynamicKey, Map[String, String])]): Unit = {

    val aggState: ReadOnlyBroadcastState[Integer, Rule] = ctx.getBroadcastState(StateDescriptor.dynamicAggregateRuleMapState)
    val ruleId: Int = value._1.id
    if (aggState.contains(ruleId)) {
      RuleSateEnum.fromString(aggState.get(ruleId).getRuleState) match {
        case RuleSateEnum.START =>
          //将数据加入到对应 datasource 和 时间 内
          val timeStamp: Long = value._2.get("timestamp").toLong / (1 * 60 * 1000) * 1 * 60 * 1000
          if (detailState.get(timeStamp) == null) {
            detailState.put(timeStamp, new ArrayBuffer[Map[String, String]]())
          }
          detailState.get(timeStamp).append(value._2)
          // 注册当前时间下一分钟的开始时间的定时器
          ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() / (1 * 60 * 1000) * 1 * 60 * 1000 + 1 * 60 * 1000)
        case _ =>
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
    val time = timestamp - 60 * 1000

    val rule = ctx.getBroadcastState(StateDescriptor.dynamicAggregateRuleMapState).get(ctx.getCurrentKey.id)

    val aggFuncList = rule.getAggregatorFun.filter(_.getDatasource.equals(JSON.parseObject(ctx.getCurrentKey.key).get("datasource")))

    if (ruleIsAvailable(rule) && aggFuncList.nonEmpty) {

      // 清除超时的状态
      detailState.keys()
        .filter(key => key <= time - aggFuncList.map(fun => fun.getWindow).max * 60 * 1000)
        .foreach(detailState.remove)

      // 当前时间点所计算出的结果数据
      val aggResultMap = new HashMap[String, Map[String, String]]()

      aggFuncList.foreach(fun => {
        val window = fun.getWindow
        val valueList = detailState.iterator()
          .filter(entry => entry.getKey > time - window * 60 * 1000 && entry.getKey <= time)
          .flatMap(_.getValue)
          .toList
        if (valueList.nonEmpty) aggResultMap.put(fun.getAliasName, AggregationFunction.calculate(fun, valueList))
      })

      //查出后置函数中datasource源为当前源的数据函数,通过别名对比
      val postFuncList = rule.getPostAggregatorFun.filter(fun => {
        aggResultMap.containsKey(fun.getFields.get(0).getFieldName)
      })

      postFuncList.foreach(fun => {
        val fields = fun.getFields
        if (!aggState.contains(fun.getAliasName)) {
          aggState.put(fun.getAliasName, new HashMap[Long, Map[String, String]])
        }
        fields.size() match {
          case 2 =>
            val firstAggResult = aggResultMap.get(fields.get(0).getFieldName).map {
              item => (JSON.parseObject(item._1), item._2)
            }
            aggResultMap.get(fields.get(1).getFieldName).map {
              item => (JSON.parseObject(item._1), item._2)
            }.foreach(entry => {
              aggState.get(fun.getAliasName).put(time,
                firstAggResult.filter(item => relationJSON(entry._1, item._1))
                  .map(item => (item._1.toJSONString, postCalculate(item._2, entry._2, fun.getOperator)))
              )
            })
          case 1 =>
            aggState.get(fun.getAliasName).put(time, aggResultMap.get(fields.get(0).getFieldName))
          case _ =>
        }
      })

      println(s"时间戳: $time => 明细数据为 ${detailState.get(time)}")
      println(s"时间戳: $time => 中间数据为 $aggResultMap")
      println(s"时间戳: $time => 结果数据为 ${aggState.iterator().toList}")

    } else {
      detailState.clear()
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

  def relationJSON(first: JSONObject, second: JSONObject): Boolean = {
    var result = true
    first.foreach(item => if (!second.contains(item._1) || !second.get(item._1).equals(item._2)) result = false)
    result
  }

  def postCalculate(firstValue: String, secondValue: String, operator: String): String = {
    val df = new DecimalFormat("#.0000")
    val result = ComputeEnum.fromString(operator) match {
      case ComputeEnum.addition => firstValue.toDouble + secondValue.toDouble
      case ComputeEnum.subtraction => firstValue.toDouble - secondValue.toDouble
      case ComputeEnum.division => df.format(firstValue.toDouble / secondValue.toDouble).toDouble
      case ComputeEnum.multiplication => firstValue.toDouble + secondValue.toDouble
    }
    result.toString
  }

}