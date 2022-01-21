package com.lb.scala.flink114.datastream

import com.lb.scala.flink114.utils.ConfigProperties
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.{BulkWriter, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner

/**
 * @ClassName Kafka2Hdfs
 * @Description @TODO
 * @Author LiuBing
 * @Date 2021/11/16 14:25
 * @Version 1.0
 * */
object Kafka2Hdfs {

  val log = LoggerFactory.getLogger(Kafka2Hdfs.getClass)
  val env = StreamExecutionEnvironment.createLocalEnvironment(4)

  // 返回集群执行环境，将Jar提交到远程服务器。需要在调用时指定JobManager的IP和端口号，并指定要在集群中运行的Jar包。
  // val env = StreamExecutionEnvironment.createRemoteEnvironment("cdh1", 8081, "/Users/liubing/IdeaProjects/lb_flink1_14/target/lb_flink1_14-1.0-SNAPSHOT.jar")

  def main(args: Array[String]): Unit = {

    //构建FlinkKafkaConsumer
    val myConsumer = new FlinkKafkaConsumer[String]("lb2", new SimpleStringSchema(), ConfigProperties.getKafkaProperties("123"))

    //指定偏移量
    // myConsumer.setStartFromEarliest()      // 尽可能从最早的记录开始
    // myConsumer.setStartFromLatest()        // 从最新的记录开始
    // myConsumer.setStartFromTimestamp(111)  // 从指定的时间开始（毫秒）
    // myConsumer.setStartFromGroupOffsets()  // 默认的方法

    val kafka_data = env.addSource(myConsumer)
      .flatMap(_.split(",", -1)).map { x =>
      UserParquetToStringSchema(x)
    } //.countWindowAll(100L )

    env.enableCheckpointing(5000)
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)

    val config = OutputFileConfig
      .builder()
      .withPartPrefix("prefix")
      .withPartSuffix(".ext")
      .build()

    // forBulkFormat  按列写入
    // forRowFormat   按行写入
    val sink: StreamingFileSink[UserParquetToStringSchema] = StreamingFileSink
      .forBulkFormat(new Path("hdfs://cdh1:8020/user/liubing/flink/kafka2hdfs"),
        ParquetAvroWriters.forReflectRecord(classOf[UserParquetToStringSchema])
          .asInstanceOf[BulkWriter.Factory[UserParquetToStringSchema]])

      //.withBucketAssigner(new DateTimeBucketAssigner[UserParquetSchema]("yyyy-MM-dd-HH-mm")) // flink 默认
      .withBucketAssigner(new DateTimeBucketAssigner[UserParquetToStringSchema]("yyyyMMddHH"))
      //.withRollingPolicy(OnCheckpointRollingPolicy.build())

      .withOutputFileConfig(config)
      .build()

    kafka_data.print()

    kafka_data.addSink(sink).name("save hdfs")

    env.execute("Kafka 2 HDFS FlinkDemo")
  }
}

case class UserParquetSchema(id: BigInt, user: String, age: Int, salary: Double)

case class UserParquetToStringSchema(user: String)