package com.xinye.operator

import com.xinye.config.state.StateDescriptor
import com.xinye.enums.{OperatorEnum, RuleSateEnum}
import com.xinye.operator.pojo.DynamicKey
import com.xinye.base.Rule

import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import java.util.Map

class DynamicTransFormFunction extends BroadcastProcessFunction[(DynamicKey, Map[String, String]), Rule, (DynamicKey, Map[String, String])] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DynamicTransFormFunction])

  override def processElement(metric: (DynamicKey, Map[String, String]),
                              ctx: BroadcastProcessFunction[(DynamicKey, Map[String, String]), Rule, (DynamicKey, Map[String, String])]#ReadOnlyContext,
                              out: Collector[(DynamicKey, Map[String, String])]): Unit = {

    val dynamicTransFormState = ctx.getBroadcastState(StateDescriptor.dynamicTransFormRuleMapState)

    val ruleId = metric._1.id

    val metrics = metric._2

    if (dynamicTransFormState.contains(ruleId)) {
      val rule = dynamicTransFormState.get(ruleId)
      RuleSateEnum.fromString(rule.getRuleState) match {
        case RuleSateEnum.ACTIVE =>
          val transFormList = rule.getTransForm.asScala
          if (transFormList.nonEmpty) {
            transFormList.foreach(t => {
              val key = t.getKey
              val operator = t.getOperator
              val target = t.getValue
              val aliasName = t.getAliasName
              val value = DynamicKeyedMapFunction.getUniqueKey(key, metrics).get.toString
              OperatorEnum.fromString(operator) match {
                case OperatorEnum.REGEX =>
                  val transFormOpthion = TransFormOperator.regex(value, target.toString)
                  if (transFormOpthion.isDefined) {
                    metrics.put(if (aliasName == null) key else aliasName, transFormOpthion.get)
                    out.collect(metric)
                  } else {
                    logger.warn(s"${ruleId}规则提供的正则表达式未解析到数据！")
                  }
                case OperatorEnum.REGEXP_EXTRACT =>

                case OperatorEnum.REGEXP_REPLACE =>
              }
            })

          } else {
            out.collect(metric)
          }
        case _ =>
      }
    }

  }

  override def processBroadcastElement(value: Rule,
                                       ctx: BroadcastProcessFunction[(DynamicKey, Map[String, String]), Rule, (DynamicKey, Map[String, String])]#Context,
                                       out: Collector[(DynamicKey, Map[String, String])]): Unit = {
    StateDescriptor.changeBroadcastState(value, ctx.getBroadcastState(StateDescriptor.dynamicTransFormRuleMapState))
    ctx.output(OutPutUtils.dynamicTransFormRuleOutput, value)
  }

  override def close(): Unit = super.close()
}
