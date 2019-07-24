#!/usr/bin/env bash
flink run -s /home/shuai/flink-check/cd2f1a590a03b9bb68f8fe0745e49f9a/chk-63/_metadata \
-c org.siys.learn.flink.OperateKafka  \
/home/shuai/IdeaProjects/test-flink/operate-kafka/target/operate-kafka-1.0.0.jar