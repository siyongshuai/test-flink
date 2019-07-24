package org.siys.learn.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object OperateKafka {


  private val DEFAULT_KAFKA_BROKER = "127.0.0.1:9092"
  private val DEFAULT_GROUP_ID = "group_flink_operate_kafka"
  private val DEFAULT_READ_TOPIC = "flink_read_topic"
  private val DEFAULT_SEND_TOPIC = "flink_send_topic"
  private val DEFAULT_CHECK_PATH = "file:///home/shuai/flink-check"

  def main(args: Array[String]): Unit = {

    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val kafkaBroker = parameterTool.get("broker-list", DEFAULT_KAFKA_BROKER)
    val groupId = parameterTool.get("groupId", DEFAULT_GROUP_ID)
    val readTopic = parameterTool.get("read-topic", DEFAULT_READ_TOPIC)
    val sendTopic = parameterTool.get("send-topic", DEFAULT_SEND_TOPIC)
    val checkPointPath = parameterTool.get("checkpoint", DEFAULT_CHECK_PATH)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(10000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(6000)

    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(10)

    env.setStateBackend(new FsStateBackend(checkPointPath, true))
    val kafkaProps = new Properties()
    kafkaProps.setProperty("auto.offset.reset", "earliest")
    kafkaProps.setProperty("bootstrap.servers", kafkaBroker)
    kafkaProps.setProperty("group.id", groupId)

    val transaction: DataStream[String] = env
      .addSource(
        new FlinkKafkaConsumer011[String](readTopic, new SimpleStringSchema(), kafkaProps)
      )


    //    transaction.print()
    val changedDataStream: DataStream[String] = transaction.map(x => (s"record_${x}", 1))
      //      .setParallelism(20)
      .keyBy(0)
      .sum(1)
      .map(x => s"${x._1}--${x._2}")

    //    changedDataStream.print()

    val kafkaProducer011 = new FlinkKafkaProducer011(kafkaBroker, sendTopic, new SimpleStringSchema())
    changedDataStream.addSink(kafkaProducer011)
    env.execute()


  }
}
