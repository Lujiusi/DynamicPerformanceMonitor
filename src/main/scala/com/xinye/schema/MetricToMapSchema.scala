package com.xinye.schema

import org.apache.commons.lang3.StringUtils

import java.nio.charset.Charset
import java.util.Map
import java.util.HashMap
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * @author daiwei04@xinye.com
 * @date 2020/11/19 16:34
 * @desc
 */
class MetricToMapSchema extends KafkaDeserializationSchema[Map[String, String]] with SerializationSchema[Map[String, String]] {

  val serialVersionUID = 1L
  @BeanProperty
  @transient var charset: Charset = _

  def this(charset: Charset) {
    this()
    this.charset = charset
  }

  override def isEndOfStream(nextElement: Map[String, String]): Boolean = false

  //  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): Map[String, String] = {
  //    val result = new HashMap[String, String]()
  //    var message = new String(record.value())
  //    //    1605750180000
  //    result.put("timestamp", message.substring(message.lastIndexOf(' ') + 1).substring(0, 13))
  //    message = message.substring(0, message.lastIndexOf(' '))
  //    result.put("appName", message.substring(message.indexOf('[') + 1, message.indexOf(']')))
  //    result.put("datasource", message.substring(0, message.indexOf('[') - 1))
  //    result.put("env", message.substring(message.indexOf(']') + 2, message.indexOf(',')))
  //    val mesArr: Array[String] = message.split("]-.{1,5},")
  //
  //    addValue(mesArr(1).substring(0, mesArr(1).lastIndexOf(' ')), result)
  //    addValue(mesArr(1).substring(mesArr(1).lastIndexOf(' ') + 1), result)
  //    result
  //  }
  //
  //  def addValue(str: String, result: Map[String, String]): Unit = {
  //    val buffer = new StringBuffer().append(str.charAt(0))
  //    for (c <- 1 until str.length - 1) {
  //      val c1 = str.charAt(c)
  //      val c2 = str.charAt(c - 1)
  //      if (','.equals(c1) && !'\\'.equals(c2) && !'}'.equals(c2) || c == str.length - 1) {
  //        val equalIndex = buffer.indexOf("=")
  //        result.put(buffer.substring(0, equalIndex), buffer.substring(equalIndex + 1))
  //        buffer.delete(0, buffer.length())
  //      } else {
  //        buffer.append(c1)
  //      }
  //    }
  //  }


  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): Map[String, String] = {
    parse(new String(record.value()))
  }

  def parse(msg: String): Map[String, String] = {
    val message = msg.replaceAll("\\\\,", "\u0001").replaceAll("\\\\n", "\u0002")
      .replace("\\=", "\u0003").replaceAll("\\\\ ", "\u0004")
    val result = new ArrayBuffer[String]()
    val time = message.substring(message.lastIndexOf(" "), message.lastIndexOf(" ") + 14).trim
    result.append(s"timestamp=$time")
    val domainInfo = message.substring(0, message.indexOf(","))
    val datasource = domainInfo.substring(0, domainInfo.indexOf("[") - 1)
    result.append(s"datasource=${datasource}")
    val env = domainInfo.split("-")(1)
    result.append(s"env=$env")
    val domain = domainInfo.substring(domainInfo.indexOf("[") + 1, domainInfo.indexOf("]"))
    result.append(s"appName=${domain}")
    val tagStr = message.substring(message.indexOf(",") + 1, message.lastIndexOf(" ")) + "&"
    val tagstr1 = tagStr.substring(0, tagStr.lastIndexOf("&"))
    val chararray = tagstr1.toCharArray
    val strlist = new ArrayBuffer[Int]()
    for (i <- 0 until chararray.size) {
      if (",".equals(chararray(i).toString) || " ".equals(chararray(i).toString)) strlist.append(i)
    }
    strlist.append(chararray.size)
    var tag = 0
    val builder = new StringBuilder
    var flag = false
    for (i <- strlist.indices if i >= tag) {
      var str = ""
      if (i == 0) {
        str = tagstr1.substring(0, strlist(0))
      } else {
        str = tagstr1.substring(strlist(i - 1), strlist(i))
      }
      breakable {
        if (flag && (" ".equals(str) || ",".equals(str) || StringUtils.isBlank(str))) {
          builder.append(str)
          break()
        }
        val dh_index = str.indexOf("=")
        val tmp: String = str.substring(dh_index + 1, dh_index + 2)
        if (tmp.equals("\"") && !flag) {
          builder.append(str.drop(str.indexOf(",")))
          flag = true
        } else if (str.endsWith("\"") && flag) {
          flag = false
          tag = i + 1
          builder.append(str.drop(str.indexOf(",")))
          result.append(builder.toString())
          builder.delete(0, builder.size)
        } else if (flag) {
          builder.append(str)
        }
        else {
          builder.append(str)
          result.append(builder.toString())
          builder.delete(0, builder.size)
        }
        if (i == strlist.size - 1) {
          val str = tagstr1.substring(strlist(i))
          builder.append(str.drop(str.indexOf(",")))
          result.append(builder.toString())
          builder.delete(0, builder.size)
        }
      }
    }
    val resultMap = new HashMap[String, String]
    result.map(str => {
      if (StringUtils.isNotBlank(str)) {
        val key = str.substring(0, str.indexOf("="))
        val index = key.indexOf(",")
        var new_key = key
        if (index == 0) new_key = key.substring(index + 1)
        val value = str.substring(str.indexOf("=") + 1)
          .replaceAll("\u0001", "\\\\,")
          .replaceAll("\u0002", "\\\\n")
          .replaceAll("\u0003", "\\=").replaceAll("\u0004", "\\\\ ")
        resultMap.put(new_key.trim, value)
      }
    })
    resultMap
  }


  override def serialize(element: Map[String, String]): Array[Byte] = element.toString.getBytes(charset)


  override def getProducedType: TypeInformation[Map[String, String]] = TypeInformation.of(new TypeHint[Map[String, String]]() {})


}
