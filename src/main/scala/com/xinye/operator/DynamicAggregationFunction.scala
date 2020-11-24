package com.xinye.operator

import java.sql.ResultSetMetaData
import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.enums.{AggregatorFunctionType, RuleSateEnum}
import com.xinye.operator.pojo.DynamicKey
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.util.Map

import org.apache.flink.api.common.time.Time

class DynamicAggregationFunction extends KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, (Long, DynamicKey, Map[String, String])] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DynamicAggregationFunction])

  val ttlConfig: StateTtlConfig = StateTtlConfig.newBuilder(Time.hours(2))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .cleanupFullSnapshot()
    .build()

  // 用于保存状态数据
  var metricByKeyState: MapState[Long, ArrayBuffer[Map[String, String]]] = _

  StateDescriptor.keyState.enableTimeToLive(ttlConfig)

  // 定义是否已经注册定时器状态
  lazy val keyState: ValueState[Int] = getRuntimeContext.getState(StateDescriptor.keyState)

  val i: AtomicInteger = new AtomicInteger(0)
  var metaData: ResultSetMetaData = _

  lazy val timeStampState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timestampTag", classOf[Long], 0))


  override def processElement(value: (DynamicKey, Map[String, String]),
                              ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, (Long, DynamicKey, Map[String, String])]#ReadOnlyContext,
                              collector: Collector[(Long, DynamicKey, util.Map[String, String])]): Unit = {

    // 当前 watermark
    val currentTime: Long = ctx.timerService().currentWatermark()
    val aggState: ReadOnlyBroadcastState[Integer, Rule] = ctx.getBroadcastState(StateDescriptor.dynamicAggregateRuleMapState)

    val ruleId: Int = value._1.id
    if (aggState.contains(ruleId)) {
      RuleSateEnum.fromString(aggState.get(ruleId).getRuleState) match {
        case RuleSateEnum.ACTIVE =>
          handleMetrics(value._2)
          if (keyState.value() == 0) {
            //注册的时间是水位线 的下一分钟  进行当前分钟的聚合计算
            ctx.timerService().registerEventTimeTimer(currentTime / (1 * 60 * 1000) * 1 * 60 * 1000 + 1 * 60 * 1000)
            //改变 是否注册 的状态
            keyState.update(1)
          }
        case _ =>
      }
    } else {
      logger.error("Rule with ID {} does not exist", ruleId)
    }

  }

  override def processBroadcastElement(rule: Rule,
                                       ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, (Long, DynamicKey, Map[String, String])]#Context,
                                       collector: Collector[(Long, DynamicKey, util.Map[String, String])]): Unit = {
    StateDescriptor.changeBroadcastState(rule, ctx.getBroadcastState(StateDescriptor.dynamicAggregateRuleMapState))
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, util.Map[String, String]), Rule, (Long, DynamicKey, util.Map[String, String])]#OnTimerContext,
                       out: Collector[(Long, DynamicKey, util.Map[String, String])]): Unit = {
    val rule = ctx.getBroadcastState(StateDescriptor.dynamicAggregateRuleMapState).get(ctx.getCurrentKey.id)

    if (ruleIsAvailable(rule)) {
      val metricList = metricByKeyState.iterator().toList
      val funList = rule.getAggregatorFun
      funList.foreach(fun => {
        val window = fun.getWindow
        val delay = fun.getDelay
        val filterList = metricList.filter(entry => {
          //根据延时已经窗口大小获取对应部分的数据左闭右开
          //                                     窗口开始时间                         窗口结束时间
          entry.getKey >= timestamp - ((delay + window) * 60 * 1000) && entry.getKey < timestamp - (delay * 60 * 1000)
        })
          .flatMap(_.getValue)

        if (filterList.nonEmpty) {
          val time = timestamp - 1 * 60 * 1000
          AggregatorFunctionType.fromString(fun.getAggregatorFunctionType) match {
            case AggregatorFunctionType.SUM =>
              val sumList = AggregationFunction.sumByRule(fun, filterList)
              sumList.foreach(map => out.collect(timestamp, ctx.getCurrentKey, map))
            //将计算结果发送到下游
            case AggregatorFunctionType.COUNT =>
              val countList = AggregationFunction.countByRule(fun, filterList)
              countList.foreach(map => {
                out.collect((time, ctx.getCurrentKey, map))
              })

            case _ => logger.error("操作符未找到")
          }

        }

        //最大延迟
        val maxDelay = funList.maxBy(fun => fun.getDelay).getDelay

        // 获取最大窗口
        val maxWindow = funList.maxBy(fun => fun.getWindow).getWindow

        metricList.filter(_.getKey < (timestamp - (maxDelay + maxWindow) * 60 * 1000))
          .foreach(entry => metricByKeyState.remove(entry.getKey))

        val newTime = timestamp + 1 * 60 * 1000

        // 继续注册下一次触发计算
        ctx.timerService().registerEventTimeTimer(newTime)

        keyState.clear()
      })
    } else {
      metricByKeyState.clear()
    }

  }

  def handleMetrics(metric: Map[String, String]): Unit = {
    val timeStamp: Long = metric.get("timestamp").toLong / (1 * 60 * 1000) * 1 * 60 * 1000
    if (metricByKeyState.get(timeStamp) == null) {
      metricByKeyState.put(timeStamp, new ArrayBuffer[Map[String, String]]())
    }
    metricByKeyState.get(timeStamp).append(metric)
  }

  def ruleIsAvailable(rule: Rule): Boolean = {
    var tag: Boolean = false
    if (rule != null) {
      tag = RuleSateEnum.fromString(rule.getRuleState) match {
        case RuleSateEnum.ACTIVE => true
        case _ => false
      }
    }
    tag
  }

}

object DynamicAggregationFunction {

  var metaMap: Map[String, String] = _

}