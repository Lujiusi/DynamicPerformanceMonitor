package com.xinye.schema

import java.nio.charset.{Charset, StandardCharsets}
import java.util.Map
import java.util.HashMap
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.JavaConversions._

/**
 * @author daiwei04@xinye.com
 * @since 2020/11/19 16:34
 */
class MetricToMapSchema extends KafkaDeserializationSchema[Map[String, String]] with SerializationSchema[Map[String, String]] {

  @transient var charset: Charset = StandardCharsets.UTF_8

  def this(charset: Charset) {
    this()
    this.charset = charset
  }

  override def isEndOfStream(nextElement: Map[String, String]): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): Map[String, String] = {
    parse(new String(record.value()))
  }

  def parse(msg: String): Map[String, String] = {
    val result = new HashMap[String, String]
    // 解析头部
    val head = msg.substring(0, msg.indexOf(","))
    if (head.contains('[')) {
      result.put("datasource", head.substring(0, msg.indexOf("[") - 1))
      result.put("appName", head.substring(head.indexOf("[") + 1, head.indexOf("]")))
      result.put("env", head.substring(head.indexOf("]-") + 2))
    } else {
      result.put("datasource", head.substring(0, head.lastIndexOf(".")))
      result.put("env", head.substring(head.lastIndexOf(".") + 1))
    }
    // 解析尾部时间
    result.put("timestamp", msg.substring(msg.lastIndexOf(" ") + 1, msg.lastIndexOf(" ") + 14))

    val body = msg.substring(msg.indexOf(",") + 1, msg.lastIndexOf(" "))
    val str = body.replaceAll("\\\\ ", "\u0001")
    val temp = new HashMap[String, String]
    val tag = str.substring(0, str.indexOf(" "))
    splitTagAndValue(tag, temp)
    val value = str.substring(str.indexOf(" ") + 1)
    splitTagAndValue(value, temp)
    temp.foreach(entry => {
      result.put(entry._1, entry._2.replaceAll("\u0002", "\\\\,")
        .replaceAll("\u0003", "\\\\=")
        .replaceAll("\u0004", "\\\\ ")
      )
    })
    result
  }

  def splitTagAndValue(str: String, result: HashMap[String, String]): Unit = {

    val msg = str.replaceAll("\u0001", "\\\\ ")
      .replaceAll("\\\\,", "\u0002")
      .replaceAll("\\\\=", "\u0003")
      .replaceAll("\\\\ ", "\u0004")

    var kvFlag = true
    var qFlag = true
    val k = new StringBuffer()
    val v = new StringBuffer()
    for (i <- msg.indices) {
      val c = msg(i)
      // 当遇到等号且不数据 "" 之内的时候 变换 kv 加减位置
      if (c == '=' && qFlag) {
        kvFlag = false
      } else {
        // 如果是 k 的范围则加载k 上面 , 生成 k
        if (kvFlag) {
          k.append(c)
        } else {
          //如果是 V 的范围则
          // 如果是 " 则反转 qFlag
          if (c == '"') {
            qFlag = if (qFlag) false else true
          }
          if (c == ',' && qFlag) {
            result.put(k.toString, v.toString)
            k.delete(0, k.length)
            v.delete(0, v.length)
            kvFlag = true
          } else if (i == msg.indices.max) {
            v.append(c)
            result.put(k.toString, v.toString)
          } else {
            v.append(c)
          }
        }
      }
    }

  }

  override def serialize(element: Map[String, String]): Array[Byte] = element.toString.getBytes(charset)

  override def getProducedType: TypeInformation[Map[String, String]] = TypeInformation.of(classOf[Map[String, String]])

}
