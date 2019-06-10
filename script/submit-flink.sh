#!/usr/bin/env bash
flink run -s /path/to/savepoint/jobid/chk-x/_metadata \
-c org.siys.learn.flink.OperateKafka  \
path/to/operate-kafka-1.0.0.jar