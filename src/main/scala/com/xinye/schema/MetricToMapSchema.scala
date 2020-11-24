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

  private val logger: Logger = LoggerFactory.getLogger(classOf[MetricToMapSchema])

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
    val mesArr: Array[String] = message.split("]-fat,")
    val head: Array[String] = mesArr(0).split("\\.\\[")
    result.put("category", head(0))
    result.put("domain", head(1))
    List(mesArr(1).substring(0, mesArr(1).lastIndexOf(' ')),
      mesArr(1).substring(mesArr(1).lastIndexOf(' ') + 1))
      .flatMap(_.split(","))
      .map(_.split("="))
      .foreach(tuple => {
        result.put(tuple(0), tuple(1))
      })
    result
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
