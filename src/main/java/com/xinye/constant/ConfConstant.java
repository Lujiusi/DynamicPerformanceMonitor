package com.xinye.constant;

/**
 * 配置常量集合
 *
 * @author daiwei04@xinye.com
 * @since 2020/12/03 23:08
 */
public interface ConfConstant {
    String SOURCE_KAFKA_BROKERS = "source.bootstrap.servers";
    String SOURCE_KAFKA_TOPIC = "source.kafka.topic";
    String SINK_KAFKA_BROKERS = "sink.bootstrap.servers";
    String SINK_KAFKA_TOPIC = "sink.kafka.topic";
    String JOB_NAME = "job.name";
    String KAFKA_GROUP_ID = "group.id";
    String CHECKPOINT_DIR = "checkpoint.dir";
}
