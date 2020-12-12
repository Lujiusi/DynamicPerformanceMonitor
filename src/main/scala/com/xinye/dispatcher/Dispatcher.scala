package com.xinye.dispatcher

import com.xinye.base.Rule
import com.xinye.constant.ConfConstant
import com.xinye.pojo.{AlarmMessage, DynamicKey}
import com.xinye.operator.{DynamicAggregationFunction, DynamicKeyedMapFunction}
import com.xinye.schema.{MetricToMapSchema, RuleSchema}
import com.xinye.state.StateDescriptor

import scala.beans.BeanProperty
import java.util.{Map, Properties}
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010, KafkaDeserializationSchema}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author daiwei04@xinye.com
 * @since 2020/11/19 11:23
 */
class Dispatcher {

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
    val ruleStream: BroadcastStream[Rule] = env.addSource(getRuleSource(prop))
      .setParallelism(1)
      .assignTimestampsAndWatermarks(new IngestionTimeExtractor[Rule])
      .broadcast(StateDescriptor.ruleState)

    // 获取 数据流
    val dataStream: DataStream[Map[String, String]] = env.addSource(getDataSource(prop)).rebalance

    val keyedStream: DataStream[(DynamicKey, Map[String, String])] = dataStream.connect(ruleStream)
      .process(new DynamicKeyedMapFunction)
      .uid("DynamicKeyed")
      .name("Dynamic Keyed Function")

    val aggregateStream: DataStream[AlarmMessage] = keyedStream
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(DynamicKey, Map[String, String])](Time.minutes(1)) {
        override def extractTimestamp(t: (DynamicKey, Map[String, String])): Long = t._2.getOrDefault("timestamp", "0").toLong
      })
      .keyBy(_._1)
      .connect(ruleStream)
      .process(new DynamicAggregationFunction)

    aggregateStream.print("alarmMessage:")

    env.execute(prop.getProperty(ConfConstant.JOB_NAME))

  }

  def getRuleSource(prop: Properties): FlinkKafkaConsumer010[Rule] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.114.24.226:9092,10.114.24.232:9092")
    properties.setProperty("group.id", ConfConstant.KAFKA_GROUP_ID)
    getSource(prop.getProperty("source.rule.topic"), new RuleSchema(), properties)
  }

  def getDataSource(prop: Properties): FlinkKafkaConsumer010[Map[String, String]] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", prop.getProperty(ConfConstant.SOURCE_KAFKA_BROKERS))
    properties.setProperty("group.id", prop.getProperty(ConfConstant.KAFKA_GROUP_ID))
    getSource(prop.getProperty(ConfConstant.SOURCE_KAFKA_TOPIC), new MetricToMapSchema(), properties)
  }

  def getSource[T](topic: String, schema: KafkaDeserializationSchema[T] with SerializationSchema[T], prop: Properties): FlinkKafkaConsumer010[T] = {
    val source = new FlinkKafkaConsumer010(topic, schema, prop)
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
