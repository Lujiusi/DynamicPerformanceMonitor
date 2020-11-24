package com.xinye.operator

import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.enums.RuleSateEnum
import com.xinye.config.RuleGauge
import com.xinye.operator.pojo.DynamicKey
import org.apache.flink.api.common.state.ReadOnlyBroadcastState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import java.util.Map

import scala.collection.JavaConversions._

class DynamicKeyedMapFunction extends BroadcastProcessFunction[java.util.Map[String, String], Rule, (DynamicKey, Map[String, String])] {

  var ruleGauge: RuleGauge = _
  private val logger: Logger = LoggerFactory.getLogger(classOf[DynamicKeyedMapFunction])

  override def open(parameters: Configuration): Unit = {
    ruleGauge = new RuleGauge
    //        getRuntimeContext.getMetricGroup ( ).gauge ( "NumberOfRules", ruleGauge )
  }

  override def processElement(value: java.util.Map[String, String],
                              ctx: BroadcastProcessFunction[java.util.Map[String, String], Rule, (DynamicKey, Map[String, String])]#ReadOnlyContext,
                              out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    // 按照 分组字段进行分组
    val ruleState: ReadOnlyBroadcastState[Integer, Rule] = ctx.getBroadcastState(StateDescriptor.dynamicKeyedMapState)
    setKeyForEventMetrics(value, ruleState, out)
  }

  override def processBroadcastElement(value: Rule,
                                       ctx: BroadcastProcessFunction[java.util.Map[String, String], Rule, (DynamicKey, Map[String, String])]#Context,
                                       out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    StateDescriptor.changeBroadcastState(value, ctx.getBroadcastState(StateDescriptor.dynamicKeyedMapState))
  }

  override def close(): Unit = super.close()

  def setKeyForEventMetrics(metrics: java.util.Map[String, String],
                            states: ReadOnlyBroadcastState[Integer, Rule],
                            out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    // 遍历状态中所有的 rule
    states.immutableEntries().iterator()
      .foreach { entry => {
        val rule = entry.getValue
        RuleSateEnum.fromString(rule.getRuleState) match {
          case RuleSateEnum.ACTIVE =>
            // 和当前 rule 有关的 数据 才会被发送到下游
            if (rule.getCategory.equalsIgnoreCase(metrics.get("category"))) {
              import scala.collection.JavaConverters._
              val uniqueKey = rule.getUniqueKey.asScala.toArray
              val buffer = new ArrayBuffer[String]()
              for (i <- uniqueKey.indices) {
                val tempKey: Option[Any] = DynamicKeyedMapFunction.getUniqueKey(uniqueKey(i), metrics)
                if (tempKey.isDefined) {
                  buffer.append(tempKey.get.toString)
                } else {
                  logger.warn("{}该数据获取不到对应的key{}", metrics, i)
                }
              }
              if (buffer.nonEmpty) {
                val dynamicKey: DynamicKey = DynamicKey(rule.getRuleID, buffer.mkString(","))
                out.collect((dynamicKey, metrics))
              }
            }
          case _ =>
        }
      }
      }
  }

}

object DynamicKeyedMapFunction {

  private val log: Logger = LoggerFactory.getLogger(classOf[DynamicKeyedMapFunction])

  /*
    参数1代表取数字段名
    参数2代表每条数据
 */
  def getUniqueKey(str: String, metrics: java.util.Map[String, String]): Option[Any] = {
    var result = Option[Any](null)
    if (str != null) {
      if (metrics.containsKey(str)) {
        result = Option(metrics.get(str))
      } else {
        log.error("{}该key可能存在定位可能出现错误请重新编写规则！", str)
      }
    }
    result
  }

}
