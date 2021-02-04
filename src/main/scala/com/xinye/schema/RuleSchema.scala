package com.xinye.schema

import java.nio.charset.{Charset, StandardCharsets}
import com.alibaba.fastjson.JSON
import com.xinye.base.Rule
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._

/**
 * @author daiwei04@xinye.com
 * @since 2020/11/19 14:17
 */
class RuleSchema extends KafkaDeserializationSchema[Rule] with SerializationSchema[Rule] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[RuleSchema])

  @transient var charset: Charset = StandardCharsets.UTF_8

  def this(charset: Charset) {
    this()
    this.charset = charset
  }

  override def isEndOfStream(nextElement: Rule): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): Rule = {
    val message = new String(record.value())
    var rule: Rule = null
    try {
      rule = JSON.parseObject(message, classOf[Rule])
      rule.getFilters
        .filter(entry => entry._1 == null || entry._2.isEmpty)
        .keys
        .foreach(rule.getFilters.remove)
      logger.info("Rule Received：{}", rule)
    } catch {
      case e: Exception =>
        logger.error("格式化异常： " + message)
        e.printStackTrace()
    }
    if (rule.getRuleID != null && rule.getSink != null && rule.getAlarmRule != null) {
      rule
    } else {
      null
    }
  }

  override def serialize(element: Rule): Array[Byte] = element.toString.getBytes(charset)

  override def getProducedType: TypeInformation[Rule] = TypeInformation.of(classOf[Rule])

}
