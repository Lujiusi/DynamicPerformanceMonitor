package com.xinye.dispatcher

import com.alibaba.fastjson.JSONObject
import com.xinye.base.Rule
import com.xinye.constant.ConfConstant
import com.xinye.pojo.DynamicKey
import com.xinye.operator.{DynamicAggregationFunction, DynamicKeyedMapFunction}
import com.xinye.schema.{MetricToMapSchema, RuleSchema}
import com.xinye.state.StateDescriptor

import scala.beans.BeanProperty
import scala.collection.JavaConversions._
import java.util.{HashMap, Map, Properties}
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, KafkaDeserializationSchema}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

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
    env.setParallelism(1)
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

    //    dataStream.print()

    val keyedStream: DataStream[(DynamicKey, Map[String, String])] = dataStream.connect(ruleStream)
      .process(new DynamicKeyedMapFunction)
      .uid("DynamicKeyed")
      .name("Dynamic Keyed Function")

    val aggregateStream: DataStream[JSONObject] = keyedStream
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(DynamicKey, Map[String, String])](Time.minutes(1)) {
        override def extractTimestamp(t: (DynamicKey, Map[String, String])): Long = t._2.getOrDefault("timestamp", "0").toLong
      })
      .keyBy(_._1)
      .connect(ruleStream)
      .process(new DynamicAggregationFunction)

    aggregateStream.print("alarmMessage: ")


    //    aggregateStream
    //      .keyBy(_.getString("category"))
    //      .connect(ruleStream)
    //      .process(new DynamicScreenAlarmData)
    //      .print("alarmMessage:")


    //    aggregateStream.addSink(new RichSinkFunction[JSONObject] {
    //
    //      private val sinkMap: Map[String, KafkaProducer[String, String]] = new HashMap[String, KafkaProducer[String, String]]()
    //
    //      private def getKafkaProducer(bs: String): KafkaProducer[String, String] = {
    //        if (sinkMap.get(bs) == null) {
    //          sinkMap.put(bs, createKafkaProducer(bs))
    //        }
    //        sinkMap.get(bs)
    //      }
    //
    //      def createKafkaProducer(bs: String): KafkaProducer[String, String] = {
    //        val sinkProp = new Properties()
    //        sinkProp.put("bootstrap.servers", bs)
    //        sinkProp.put("acks", "1")
    //        sinkProp.put("key.serializer", classOf[StringSerializer].getName)
    //        sinkProp.put("value.serializer", classOf[StringSerializer].getName)
    //        new KafkaProducer[String, String](sinkProp)
    //      }
    //
    //      override def invoke(value: JSONObject, context: SinkFunction.Context[_]): Unit = {
    //        val sink = value.get("sink").asInstanceOf[JSONObject]
    //        value.remove("sink")
    //        val bs = sink.getString("bootstrap.servers")
    //        val topic = sink.getString("topic")
    //        val kafkaProducer = getKafkaProducer(bs)
    //        kafkaProducer.send(new ProducerRecord(topic, value.toJSONString))
    //      }
    //
    //      override def close(): Unit = {
    //        sinkMap.values().foreach(_.close())
    //      }
    //
    //    })

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

}

object Dispatcher {

  def apply(prop: Properties): Dispatcher = new Dispatcher(prop)

}