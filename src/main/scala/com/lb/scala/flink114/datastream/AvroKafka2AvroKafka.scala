package com.lb.scala.flink114.datastream

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord}
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.formats.avro.{AvroDeserializationSchema, AvroSerializationSchema}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.slf4j.LoggerFactory
import org.apache.kafka.clients.consumer._

import java.io.File
import java.util.Properties

object AvroKafka2AvroKafka {

  val log = LoggerFactory.getLogger(AvroKafka2AvroKafka.getClass)

  // kafka配置
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
  properties.setProperty("zookeeper.connect", "127.0.0.1:2181")
  properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  properties.setProperty("auto.offset.reset", "earliest")
  properties.setProperty("group.id", "12333")


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 根据schema文件
    // val path = AvroKafka2AvroKafka.getClass.getClassLoader().getResource("avro/user.avsc").getPath()
    val avroDeserializationSchema =
    AvroDeserializationSchema.forGeneric(new Schema.Parser().parse(new File("/Users/liubing/demo/avro/user.avsc")))

    // 根据pojo类解析
    // val avroDeserializationSchema_pojo = AvroDeserializationSchema.forSpecific(classOf[User])

    //构建FlinkKafkaConsumer
    val myConsumer = new FlinkKafkaConsumer[GenericRecord]("lb3", avroDeserializationSchema, properties)

    // 指定offset消费位置
    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    specificStartOffsets.put(new KafkaTopicPartition("lb3", 0), 348L)
    //specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L)
    // specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L)

    myConsumer.setStartFromSpecificOffsets(specificStartOffsets)

    //指定偏移量
    // myConsumer.setStartFromEarliest()      // 尽可能从最早的记录开始
    // myConsumer.setStartFromLatest()        // 从最新的记录开始
    // myConsumer.setStartFromTimestamp(111)  // 从指定的时间开始（毫秒）
    // myConsumer.setStartFromGroupOffsets()  // 默认的方法

    val kafka_data = env.addSource(myConsumer).map {
      x =>
        println(x.getSchema)
        x.get("age")
        x
    }

    env.enableCheckpointing(5000)
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)

    kafka_data.print()

    // 根据pojo类序列化
    val avroSerializationSchema = AvroSerializationSchema.forGeneric(new Schema.Parser().parse(new File("/Users/liubing/demo/avro/user.avsc")))

    val myProducer = new FlinkKafkaProducer[GenericRecord](
      "lb5", // 目标 topic
      avroSerializationSchema, // 序列化 schema
      properties // producer 配置
     // , FlinkKafkaProducer.Semantic.EXACTLY_ONCE // 容错
    )
    myProducer.setWriteTimestampToKafka(true) // 给每条记录设设置时间戳
    myProducer.setLogFailuresOnly(false) // 设置是否在 Producer 发生异常时仅仅记录日志
    myProducer.setTransactionalIdPrefix("lb") // 设置自定义的 transactional.id 前缀
    myProducer.ignoreFailuresAfterTransactionTimeout() // 在恢复时忽略事务超时异常

    kafka_data.addSink(myProducer)

    env.execute()
  }
}