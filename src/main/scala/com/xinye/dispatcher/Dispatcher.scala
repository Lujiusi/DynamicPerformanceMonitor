package com.xinye.dispatcher

import java.util
import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.constant.ConfConstant
import com.xinye.pojo.DynamicKey
import com.xinye.operator.{DynamicAggregationFunction, DynamicKeyedMapFunction}
import com.xinye.schema.{MetricToMapSchema, RuleSchema}
import com.xinye.service.StreamEnv
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

import scala.beans.BeanProperty
import java.util.{Map, Properties}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

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

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val backend = new FsStateBackend(prop.getProperty(ConfConstant.CHECKPOINT_DIR), true)
    env.setStateBackend(backend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(60 * 1000)
    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointTimeout(10 * 60 * 1000)
    checkpointConfig.setMaxConcurrentCheckpoints(3)


    // 获取规则流
    val ruleStream: DataStream[Rule] = env.addSource(getRuleSource(prop)).setParallelism(1)

    val dynamicKeyStream: BroadcastStream[Rule] = ruleStream.broadcast(StateDescriptor.dynamicKeyedMapState)

    val dynamicAggregateRuleStream: BroadcastStream[Rule] = ruleStream.broadcast(StateDescriptor.dynamicAggregateRuleMapState)

    ruleStream.print("规则流")

    // 获取 数据流
    val dataStream: DataStream[Map[String, String]] = env.addSource(getDataSource(prop)).rebalance

    val keyedStream: DataStream[(DynamicKey, Map[String, String])] = dataStream.connect(dynamicKeyStream)
      .process(new DynamicKeyedMapFunction)
      .uid("DynamicKeyed")
      .name("Dynamic Keyed Function")

    val aggregateStream: DataStream[(DynamicKey, Map[String, String])] = keyedStream
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(DynamicKey, util.Map[String, String])](Time.minutes(1)) {
        override def extractTimestamp(t: (DynamicKey, util.Map[String, String])): Long = t._2.getOrDefault("timestamp", "0").toLong
      })
      .keyBy(_._1)
      .connect(dynamicAggregateRuleStream)
      .process(new DynamicAggregationFunction)

    aggregateStream.print()

    env.execute(prop.getProperty(ConfConstant.JOB_NAME))

  }

  def getRuleSource(prop: Properties): FlinkKafkaConsumer010[Rule] = {

    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "10.114.24.226:9092,10.114.24.232:9092")
    properties.setProperty("group.id", ConfConstant.KAFKA_GROUP_ID)

    val source = new FlinkKafkaConsumer010[Rule](prop.getProperty("source.rule.topic"), new RuleSchema(), properties)

    source.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Rule]() {

      override def getCurrentWatermark: Watermark = {
        new Watermark(System.currentTimeMillis())
      }

      override def extractTimestamp(element: Rule, previousElementTimestamp: Long): Long = {
        previousElementTimestamp
      }

    })
    source.setStartFromLatest()
    source
  }

  def getDataSource(prop: Properties): FlinkKafkaConsumer010[util.Map[String, String]] = {
    val kafkaProp = new Properties()
    kafkaProp.setProperty("bootstrap.servers", prop.getProperty(ConfConstant.SOURCE_KAFKA_BROKERS))
    kafkaProp.setProperty("group.id", prop.getProperty(ConfConstant.KAFKA_GROUP_ID))
    kafkaProp.put("enable.auto.commit", "true")
    kafkaProp.put("auto.commit.interval.ms", "10000")
    val source = new FlinkKafkaConsumer010(prop.getProperty(ConfConstant.SOURCE_KAFKA_TOPIC), new MetricToMapSchema(), kafkaProp)
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
