package com.xinye.schema

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.nio.charset.Charset

import com.alibaba.fastjson.JSON
import com.xinye.base.Rule
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.beans.BeanProperty

/**
 * @author daiwei04@xinye.com
 * @date 2020/11/19 14:17
 * @desc
 */
class RuleSchema extends KafkaDeserializationSchema[Rule] with SerializationSchema[Rule] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[RuleSchema])

  @BeanProperty
  @transient var charset: Charset = _

  override def isEndOfStream(nextElement: Rule): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): Rule = {

    val message = new String(record.value())
    var rule = new Rule()
    try {
      rule = JSON.parseObject(message, classOf[Rule])
      logger.info("收到规则：{}", message)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error("格式化异常： " + message)
    }
    rule

  }

  override def serialize(element: Rule): Array[Byte] = {
    val bytes = element.toString.getBytes(charset)
    bytes
  }

  def writeObject(out: ObjectOutputStream) {
    out.defaultWriteObject()
    out.writeUTF(charset.name())
  }

  def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    val charsetName = in.readUTF()
    this.charset = Charset.forName(charsetName)
  }

  override def getProducedType: TypeInformation[Rule] = {
    TypeInformation.of(new TypeHint[Rule]() {
    })
  }

}
