package com.xinye.schema

import java.nio.charset.Charset
import java.util

import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.beans.BeanProperty

/**
 * @author daiwei04@xinye.com
 * @date 2020/11/19 16:34
 * @desc
 */
class MetricToMapSchema extends KafkaDeserializationSchema[util.Map[String, String]] with SerializationSchema[util.Map[String, String]] {

  val serialVersionUID = 1L
  @BeanProperty
  @transient var charset: Charset = _

  def this(charset: Charset) {
    this()
    this.charset = charset
  }

  override def isEndOfStream(nextElement: util.Map[String, String]): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): util.Map[String, String] = {
    val result = new util.HashMap[String, String]()
    var message = new String(record.value())
    //    1605750180000
    result.put("timestamp", message.substring(message.lastIndexOf(' ') + 1).substring(0, 13))
    message = message.substring(0, message.lastIndexOf(' '))
    result.put("appName", message.substring(message.indexOf('[') + 1, message.indexOf(']')))
    result.put("datasource", message.substring(0, message.indexOf('[') - 1))
    result.put("env", message.substring(message.indexOf(']') + 2, message.indexOf(',')))
    val mesArr: Array[String] = message.split("]-.{1,5},")

    addValue(mesArr(1).substring(0, mesArr(1).lastIndexOf(' ')), result)
    addValue(mesArr(1).substring(mesArr(1).lastIndexOf(' ') + 1), result)
    result
  }

  def addValue(str: String, result: util.Map[String, String]): Unit = {
    val buffer = new StringBuffer().append(str.charAt(0))
    for (c <- 1 until str.length - 1) {
      val c1 = str.charAt(c)
      val c2 = str.charAt(c - 1)
      if (','.equals(c1) && !'\\'.equals(c2) && !'}'.equals(c2) || c == str.length - 1) {
        val equalIndex = buffer.indexOf("=")
        result.put(buffer.substring(0, equalIndex), buffer.substring(equalIndex + 1))
        buffer.delete(0, buffer.length())
      } else {
        buffer.append(c1)
      }
    }
  }

  override def serialize(element: util.Map[String, String]): Array[Byte] = {
    val bytes = element.toString.getBytes(charset)
    bytes
  }

  override def getProducedType: TypeInformation[util.Map[String, String]] = {
    TypeInformation.of(new TypeHint[util.Map[String, String]]() {
    })
  }

}
