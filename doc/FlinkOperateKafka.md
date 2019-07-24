# flink 读写 kafka基本操作
 ```code
 val env = StreamExecutionEnvironment.getExecutionEnvironment
 val kafkaConsumer =  new FlinkKafkaConsumer011[String](readTopic, new SimpleStringSchema(), kafkaProps)
 val ds :DataStream[String]  =  env.addSource(kafkaConsumer)
 val kafkaProducer = new FlinkKafkaProducer011(kafkaBroker, sendTopic, new SimpleStringSchema())
 ds.addSink(kafkaProducer011)
 
```
# flink 实现 EXACTLY_ONCE

## 实现方式
Checkpoint

### Checkpoint 相关设置
name | method | must |remarks
----------|----------|----------|-----------
enable    |  env.enableCheckpointing(10000)    | yes |启用checkpoint 并设置频率,频率可以不设,默认500ms
mode      |  chkConfig.setCheckpointingMode(mode:CheckpointingMode) |yes  | AT_LEAST_ONCE,EXACTLY_ONCE
cancel    |  chkConfig.enableExternalizedCheckpoints( cleanupMode:ExternalizedCheckpointCleanup) | no | RETAIN_ON_CANCELLATION,DELETE_ON_CANCELLATION
path      |  env.setStateBackend(new FsStateBackend(checkPointPath, true))| yes | 
timeOut   | chkConfig.setCheckpointTimeout(6000) | yes | 
maxAttempts|chkConfig.setMaxConcurrentCheckpoints(10) | no | The maximum number of concurrent attempts must be at least one

 