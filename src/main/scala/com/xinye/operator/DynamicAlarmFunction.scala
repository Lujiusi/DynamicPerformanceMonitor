package com.xinye.operator

import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.enums.LimitOperatorEnum
import org.apache.flink.api.common.state.ReadOnlyBroadcastState
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.parsing.json.JSONObject
import scala.util.control.Breaks._

/**
 * @program: RTFlinkJobs
 * @description: ${description}
 * @author: liangwei
 * @create: 2020-06-11 22:45
 * */
class DynamicAlarmFunction extends KeyedBroadcastProcessFunction[Int, ((Long, Int), ArrayBuffer[mutable.Map[String, Any]]), Rule, String] {

  private val log = LoggerFactory.getLogger(classOf[DynamicAlarmFunction])

  override def processElement(value: ((Long, Int), ArrayBuffer[mutable.Map[String, Any]]),
                              ctx: KeyedBroadcastProcessFunction[Int, ((Long, Int), ArrayBuffer[mutable.Map[String, Any]]), Rule, String]#ReadOnlyContext,
                              out: Collector[String]): Unit = {

    val alarmState: ReadOnlyBroadcastState[Integer, Rule] = ctx.getBroadcastState(StateDescriptor.dynamicAlarmRuleMapState)
    // 规则 ID
    val rule: Rule = alarmState.get(value._1._2)

    val rules = rule.getAlarmRule

    // 将指标写出
    val metrics = value._2

    rules.asScala.foreach(alarms => {

      var tag = false

      val alarmId = alarms.getAlarmId

      val fields = alarms.getFields

      val ruleId = value._1._2

      val map = new mutable.HashMap[String, Any]()

      map.put("timestamp", value._1._1)

      map.put("ruleId", ruleId)

      map.put("alarmId", alarmId)

      // 获取 rule  中 所有的报警字段
      val alarmColumns = fields.toList.map(alarm => alarm.getAlarmColumn)

      //筛选出含有全部报警字段的 数据
      val metricList = metrics.toList.filter(metric => metric.keySet.containsAll(alarmColumns))

      breakable(

        for (field <- fields) {

          val alarmColumn = field.getAlarmColumn

          val compareOperator = field.getCompareOperator

          val target = field.getTarget

          if (metricList.nonEmpty) {
            val metric = metricList.head
            map.putAll(metric)
            val metricValue = metric(alarmColumn).toString.toDouble
            LimitOperatorEnum.fromString(compareOperator) match {

              case LimitOperatorEnum.GREATER => if (metricValue > target) tag = true else tag = false

              case LimitOperatorEnum.GREATER_EQUAL => if (metricValue >= target) tag = true else tag = false

              case LimitOperatorEnum.LESS => if (metricValue < target) tag = true else tag = false

              case LimitOperatorEnum.LESS_EQUAL => if (metricValue <= target) tag = true else tag = false

              case _ => log.error(s"$compareOperator 该比较方式暂不支持！")

            }

            //不满足任何一个条件 就不做继续进行了 不会预警
            if (!tag) break()
          }
        }
      )

      //满足所有的条件 则 将数据输出  造成预警
      if (tag) {
        val result = JSONObject(map.toMap).toString()
        out.collect(result)
      }
    })
  }

  // 动态处理 规则流数据 , 用于更改 状态
  override def processBroadcastElement(value: Rule,
                                       ctx: KeyedBroadcastProcessFunction[Int, ((Long, Int), ArrayBuffer[mutable.Map[String, Any]]), Rule, String]#Context,
                                       out: Collector[String]): Unit = {
    StateDescriptor.changeBroadcastState(value, ctx.getBroadcastState(StateDescriptor.dynamicAlarmRuleMapState))
  }
}
