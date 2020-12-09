package com.xinye.schema

import java.nio.charset.Charset
import java.util.Map
import java.util.HashMap
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.beans.BeanProperty
import scala.collection.JavaConversions._

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

  //  def parse(msg: String): Map[String, String] = {
  //    val message = msg.replaceAll("\\\\,", "\u0001").replaceAll("\\\\n", "\u0002")
  //      .replace("\\=", "\u0003").replaceAll("\\\\ ", "\u0004")
  //    val result = new ArrayBuffer[String]()
  //    val time = message.substring(message.lastIndexOf(" "), message.lastIndexOf(" ") + 14).trim
  //    result.append(s"timestamp=$time")
  //    val domainInfo = message.substring(0, message.indexOf(","))
  //    val datasource = domainInfo.substring(0, domainInfo.indexOf("[") - 1)
  //    result.append(s"datasource=${datasource}")
  //    val env = domainInfo.split("-")(1)
  //    result.append(s"env=$env")
  //    val domain = domainInfo.substring(domainInfo.indexOf("[") + 1, domainInfo.indexOf("]"))
  //    result.append(s"appName=${domain}")
  //    val tagStr = message.substring(message.indexOf(",") + 1, message.lastIndexOf(" ")) + "&"
  //    val tagstr1 = tagStr.substring(0, tagStr.lastIndexOf("&"))
  //    val chararray = tagstr1.toCharArray
  //    val strlist = new ArrayBuffer[Int]()
  //    for (i <- 0 until chararray.size) {
  //      if (",".equals(chararray(i).toString) || " ".equals(chararray(i).toString)) strlist.append(i)
  //    }
  //    strlist.append(chararray.size)
  //    var tag = 0
  //    val builder = new StringBuilder
  //    var flag = false
  //    for (i <- strlist.indices if i >= tag) {
  //      var str = ""
  //      if (i == 0) {
  //        str = tagstr1.substring(0, strlist(0))
  //      } else {
  //        str = tagstr1.substring(strlist(i - 1), strlist(i))
  //      }
  //      breakable {
  //        if (flag && (" ".equals(str) || ",".equals(str) || StringUtils.isBlank(str))) {
  //          builder.append(str)
  //          break()
  //        }
  //        val dh_index = str.indexOf("=")
  //        val tmp: String = str.substring(dh_index + 1, dh_index + 2)
  //        if (tmp.equals("\"") && !flag) {
  //          builder.append(str.drop(str.indexOf(",")))
  //          flag = true
  //        } else if (str.endsWith("\"") && flag) {
  //          flag = false
  //          tag = i + 1
  //          builder.append(str.drop(str.indexOf(",")))
  //          result.append(builder.toString())
  //          builder.delete(0, builder.size)
  //        } else if (flag) {
  //          builder.append(str)
  //        }
  //        else {
  //          builder.append(str)
  //          result.append(builder.toString())
  //          builder.delete(0, builder.size)
  //        }
  //        if (i == strlist.size - 1) {
  //          val str = tagstr1.substring(strlist(i))
  //          builder.append(str.drop(str.indexOf(",")))
  //          result.append(builder.toString())
  //          builder.delete(0, builder.size)
  //        }
  //      }
  //    }
  //    val resultMap = new HashMap[String, String]
  //    result.map(str => {
  //      if (StringUtils.isNotBlank(str)) {
  //        val key = str.substring(0, str.indexOf("="))
  //        val index = key.indexOf(",")
  //        var new_key = key
  //        if (index == 0) new_key = key.substring(index + 1)
  //        val value = str.substring(str.indexOf("=") + 1)
  //          .replaceAll("\u0001", "\\\\,")
  //          .replaceAll("\u0002", "\\\\n")
  //          .replaceAll("\u0003", "\\=").replaceAll("\u0004", "\\\\ ")
  //        resultMap.put(new_key.trim, value)
  //      }
  //    })
  //    resultMap
  //  }


  override def serialize(element: Map[String, String]): Array[Byte] = element.toString.getBytes(charset)


  override def getProducedType: TypeInformation[Map[String, String]] = TypeInformation.of(new TypeHint[Map[String, String]]() {})


}
