package com.xinye.service

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
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  private val backend = new FsStateBackend("file:///D:/IdeaProject/DynamicPerformanceMonitor/checkpoint", true)
  env.setStateBackend(backend)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.enableCheckpointing(60 * 1000)
  private val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
  checkpointConfig.setCheckpointTimeout(10 * 60 * 1000)
  checkpointConfig.setMaxConcurrentCheckpoints(3)
}