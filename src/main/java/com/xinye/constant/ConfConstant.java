package com.xinye.constant;

/**
 * 配置常量集合
 *
 * @author daiwei04@xinye.com
 * @since 2020/12/03 23:08
 */
public interface ConfConstant {
    String SOURCE_DATA_KAFKA_BROKERS = "source.data.bootstrap.servers";
    String SOURCE_RULE_KAFKA_BROKERS = "source.rule.bootstrap.servers";
    String SOURCE_DATA_TOPIC = "source.data.topic";
    String SOURCE_RULE_TOPIC = "source.rule.topic";
    String JOB_NAME = "job.name";
    String KAFKA_GROUP_ID = "group.id";
    String CHECKPOINT_DIR = "checkpoint.dir";
}
