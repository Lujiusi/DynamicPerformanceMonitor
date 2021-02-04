package com.xinye.operator


import com.alibaba.fastjson.JSONObject
import scala.collection.JavaConversions._
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.{HashMap, Map, Properties}

/**
 * @author daiwei04@xinye.com
 * @since 2021/1/25 15:17
 */

class DynamicSinkFunction extends RichSinkFunction[JSONObject] {

  private val sinkMap: Map[String, KafkaProducer[String, String]] = new HashMap[String, KafkaProducer[String, String]]()

  private def getKafkaProducer(bs: String): KafkaProducer[String, String] = {
    if (sinkMap.get(bs) == null) {
      sinkMap.put(bs, createKafkaProducer(bs))
    }
    sinkMap.get(bs)
  }

  def createKafkaProducer(bs: String): KafkaProducer[String, String] = {
    val sinkProp = new Properties()
    sinkProp.put("bootstrap.servers", bs)
    sinkProp.put("acks", "1")
    sinkProp.put("key.serializer", classOf[StringSerializer].getName)
    sinkProp.put("value.serializer", classOf[StringSerializer].getName)
    new KafkaProducer[String, String](sinkProp)
  }

  override def invoke(value: JSONObject, context: SinkFunction.Context[_]): Unit = {
    val sink = value.get("sink").asInstanceOf[JSONObject]
    value.remove("sink")
    val bs = sink.getString("bootstrapServers")
    val topic = sink.getString("topic")
    val kafkaProducer = getKafkaProducer(bs)
    kafkaProducer.send(new ProducerRecord(topic, value.toJSONString))
  }

  override def close(): Unit = {
    sinkMap.values().foreach(_.close())
  }

}
