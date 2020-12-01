package com.xinye.dispatcher

import java.util

import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.operator.pojo.DynamicKey
import com.xinye.operator.{DynamicAggregationFunction, DynamicKeyedMapFunction, DynamicPostAggregateFun}
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

    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "10.114.24.226:9092,10.114.24.232:9092")
    properties.setProperty("rule-topic", "performance_monitor_rule")
    properties.setProperty("group.id", "dynamicAlarm_new")

    // 获取规则流
    val ruleStream: DataStream[Rule] = env.addSource(getRuleSource(properties)).setParallelism(1)

    val dynamicFilterRuleStream: BroadcastStream[Rule] = ruleStream.broadcast(StateDescriptor.dynamicFilterRuleMapState)

    val dynamicKeyStream: BroadcastStream[Rule] = ruleStream.broadcast(StateDescriptor.dynamicKeyedMapState)

    val dynamicAggregateRuleStream: BroadcastStream[Rule] = ruleStream.broadcast(StateDescriptor.dynamicAggregateRuleMapState)
    val dynamicPostAggregateRuleStream: BroadcastStream[Rule] = ruleStream.broadcast(StateDescriptor.dynamicPostAggregateRuleMapState)
    val dynamicAlarmRuleStream: BroadcastStream[Rule] = ruleStream.broadcast(StateDescriptor.dynamicAlarmRuleMapState)

    ruleStream.print("规则流")

    // 获取 数据流
    val dataStream: DataStream[Map[String, String]] = env.addSource(getDataSource(prop)).rebalance

    val keyedStream: DataStream[(DynamicKey, Map[String, String])] = dataStream.connect(dynamicKeyStream)
      .process(new DynamicKeyedMapFunction)
      .uid("DynamicKeyed")
      .name("Dynamic Keyed Function")

    keyedStream.print("分组流")

    val aggregateStream: DataStream[(DynamicKey, util.Map[String, String])] = keyedStream
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(DynamicKey, util.Map[String, String])] {

        var currentTime = 1605442872000L

        override def getCurrentWatermark: Watermark = new Watermark(currentTime)


        override def extractTimestamp(element: (DynamicKey, util.Map[String, String]), previousElementTimestamp: Long): Long = {
          currentTime = element._2.getOrDefault("timestamp", "0").toLong
          currentTime
        }
      })
      .keyBy(_._1)
      .connect(dynamicAggregateRuleStream)
      .process(new DynamicAggregationFunction)

    aggregateStream.print()
    //    aggregateStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(DynamicKey, util.Map[String, String])] {
    //
    //      var currentTime = 1605442872000L
    //
    //      override def getCurrentWatermark: Watermark = new Watermark(currentTime)
    //
    //      override def extractTimestamp(element: (DynamicKey, util.Map[String, String]), previousElementTimestamp: Long): Long = {
    //        currentTime = element._1.timestamp
    //        currentTime
    //      }
    //
    //    })
    //      .keyBy(_._1)
    //      .connect(dynamicPostAggregateRuleStream)
    //      .process(new DynamicPostAggregateFun)

    //    aggStream.connect(dynamicAlarmRuleStream).process(new DynamicAlarmFunction)

    //    aggStream.print("聚合数据:")

    env.execute("")

  }

  def getRuleSource(prop: Properties): FlinkKafkaConsumer010[Rule] = {
    val source = new FlinkKafkaConsumer010[Rule](prop.getProperty("rule-topic"), new RuleSchema(), prop)

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
    new FlinkKafkaProducer010[String]("performance_monitor_rule", new SimpleStringSchema(), prop)
  }

}

object Dispatcher {

  def apply(prop: Properties): Dispatcher = new Dispatcher(prop)

}
