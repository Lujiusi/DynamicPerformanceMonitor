package com.xinye.dispatcher

import java.util

import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.operator.pojo.DynamicKey
import com.xinye.operator.{DynamicAggregationFunction, DynamicAlarmFunction, DynamicFilterFunction, DynamicKeyedMapFunction}
import com.xinye.schema.{MetricToMapSchema, RuleSchema}
import com.xinye.service.StreamEnv
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

import scala.beans.BeanProperty
import java.util.{Map, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema

/**
 * @author daiwei04@xinye.com
 * @date 2020/11/19 11:23
 * @desc
 */

class Dispatcher extends StreamEnv {

  @transient
  @BeanProperty var prop: Properties = _

  def this(prop: Properties) {
    this()
    this.prop = prop
  }

  def run(): Unit = {

    // 获取规则流
    val ruleStream: DataStream[Rule] = env.addSource(getRuleSource(prop))

    val dynamicFilterRuleStream: BroadcastStream[Rule] = ruleStream.broadcast(StateDescriptor.dynamicFilterRuleMapState)

    val dynamicKeyStream: BroadcastStream[Rule] = ruleStream.broadcast(StateDescriptor.dynamicKeyedMapState)
    val dynamicAggregateRuleStream: BroadcastStream[Rule] = ruleStream.broadcast(StateDescriptor.dynamicAggregateRuleMapState)
    val dynamicPostAggregateRuleStream: BroadcastStream[Rule] = ruleStream.broadcast(StateDescriptor.dynamicPostAggregateRuleMapState)
    val dynamicAlarmRuleStream: BroadcastStream[Rule] = ruleStream.broadcast(StateDescriptor.dynamicAlarmRuleMapState)

    // 获取 数据流
    val dataStream: DataStream[Map[String, String]] = env.addSource(getDataSource(prop)).rebalance





    val keyedStream: DataStream[(DynamicKey, Map[String, String])] = dataStream.connect(dynamicKeyStream)
      .process(new DynamicKeyedMapFunction)
      .uid("DynamicKeyed")
      .name("Dynamic Keyed Function")

    //动态过滤
    val filterStream: DataStream[(DynamicKey, Map[String, String])] = keyedStream
      .connect(dynamicFilterRuleStream)
      .process(new DynamicFilterFunction)
      .uid("DynamicFilter")
      .name("Dynamic Filter Function")
      .rebalance


    val aggStream: DataStream[(Long, DynamicKey, Map[String, String])] = filterStream
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(DynamicKey, Map[String, String])] {

        override def getCurrentWatermark: Watermark = {
          new Watermark(1605442872000L)
        }

        override def extractTimestamp(element: (DynamicKey, Map[String, String]), previousElementTimestamp: Long): Long = {
          element._2.getOrDefault("timestamp", "0").toLong
        }
      })
      .keyBy(_._1).connect(dynamicAggregateRuleStream)
      .process(new DynamicAggregationFunction)
      .setParallelism(15)

    //    aggStream.connect(dynamicAlarmRuleStream).process(new DynamicAlarmFunction)

    dataStream.print("metric")

    env.execute("")

  }

  def getRuleSource(prop: Properties): FlinkKafkaConsumer010[Rule] = {
    val source = new FlinkKafkaConsumer010[Rule](prop.getProperty("rules-topic"), new RuleSchema(), prop)

    source.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Rule]() {

      override def getCurrentWatermark: Watermark = {
        new Watermark(System.currentTimeMillis() - 15 * 60 * 1000)
      }

      override def extractTimestamp(element: Rule, previousElementTimestamp: Long): Long = {
        previousElementTimestamp
      }

    })

    source.setStartFromLatest()
    source
  }

  def getDataSource(prop: Properties): FlinkKafkaConsumer010[util.Map[String, String]] = {
    val source = new FlinkKafkaConsumer010(prop.getProperty("data-topic"), new MetricToMapSchema(), prop)
    source.setStartFromLatest()
    source
  }

  def getAlarmSink(prop: Properties): FlinkKafkaProducer010[String] = {
    new FlinkKafkaProducer010[String](prop.getProperty(""), new SimpleStringSchema(), prop)
  }

}

object Dispatcher {

  def apply(prop: Properties): Dispatcher = new Dispatcher(prop)

}
