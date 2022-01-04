package com.lb.scala.flink114.datastream

import java.util.concurrent.TimeUnit
import com.lb.scala.flink114.utils.ConfigProperties
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer}
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._

/**
  * @ClassName Kafka2LocalFile
  * @Description @TODO
  * @Author liubing
  * @Date 2021/11/18 15:48
  * @Version 1.0
  **/
object Kafka2LocalFile extends App {

  val log = LoggerFactory.getLogger(Kafka2LocalFile.getClass)

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  //构建FlinkKafkaConsumer
  val myConsumer = new FlinkKafkaConsumer[String]("lb2", new SimpleStringSchema(), ConfigProperties.getKafkaProperties("abc"))

  //指定偏移量
  // myConsumer.setStartFromEarliest()      // 尽可能从最早的记录开始
  // myConsumer.setStartFromLatest()        // 从最新的记录开始
  // myConsumer.setStartFromTimestamp(111)  // 从指定的时间开始（毫秒）
  // myConsumer.setStartFromGroupOffsets()  // 默认的方法

  val kafka_data_source: DataStream[String] = env.addSource(myConsumer)

  env.enableCheckpointing(5000)


  // forBulkFormat  按列写入
  // forRowFormat   按行写入
  // hdfs//cdh1:8020//user/liubing/flink/kafka2hdfs
  //  /Users/liubing/demo/flink

  // 写入本地文件
  lazy val local_file_sink: StreamingFileSink[String] = StreamingFileSink
    .forRowFormat(new Path("hdfs://cdh1:8020/user/liubing/flink/kafka2hdfs"), new SimpleStringEncoder[String]("UTF-8"))
    .withRollingPolicy(
      DefaultRollingPolicy.builder()
        // 滚动策略
        .withRolloverInterval(TimeUnit.MINUTES.toMillis(2)) // 它至少包含 n 分钟的数据
        .withInactivityInterval(TimeUnit.MINUTES.toMillis(2)) // 最近 n 分钟没有收到新的记录
        .withMaxPartSize(1024 * 1024) // 文件大小达到 n GB （写入最后一条记录后）
        .build())
    .build()

  kafka_data_source.addSink(local_file_sink)

  env.execute()
}
