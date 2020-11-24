package com.xinye.service

import java.io.InputStream
import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author daiwei04@xinye.com
 * @date 2020/11/19 11:21
 * @desc
 */
trait StreamEnv {
  private val in: InputStream = classOf[StreamEnv].getClassLoader.getResourceAsStream("job-test.properties")
  private val tool: ParameterTool = ParameterTool.fromPropertiesFile(in)
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  //  private val backend = new FsStateBackend("hdfs://caasnameservice/flink-checkpoints/DynamicPerformanceMonitor", true)
  //  env.setStateBackend(backend)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getConfig.setGlobalJobParameters(tool)
  env.enableCheckpointing(60 * 1000)
  private val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
  checkpointConfig.setCheckpointTimeout(10 * 60 * 1000)
  checkpointConfig.setMaxConcurrentCheckpoints(3)
}