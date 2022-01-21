package com.lb.scala.flink114.datastream

import com.lb.scala.flink114.utils.ConfigProperties
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

import java.util.Properties

object Kafka2Kafka {

  val log = LoggerFactory.getLogger(Kafka2Hdfs.getClass)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //构建FlinkKafkaConsumer
    val myConsumer =
      new FlinkKafkaConsumer[ObjectNode]("lb4", new JSONKeyValueDeserializationSchema(true), ConfigProperties.getKafkaProperties("123"))

    //指定偏移量
    // myConsumer.setStartFromEarliest()      // 尽可能从最早的记录开始
    myConsumer.setStartFromLatest() // 从最新的记录开始
    // myConsumer.setStartFromTimestamp(111)  // 从指定的时间开始（毫秒）
    // myConsumer.setStartFromGroupOffsets()  // 默认的方法

    val kafka_data = env.addSource(myConsumer).map {
      x =>
        println(x)
        x.get("value").get("k").asText()
      // {"value":{"k":"abc"},"metadata":{"offset":46,"topic":"lb4","partition":0}}
    }

    env.enableCheckpointing(5000)
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)

    kafka_data.print()

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "localhost:9092")

    val myProducer = new FlinkKafkaProducer[String](
      "lb3", // 目标 topic
      new SimpleStringSchema(), // 序列化 schema
      properties // producer 配置
      // , FlinkKafkaProducer.Semantic.EXACTLY_ONCE  // 容错
    )
    myProducer.setWriteTimestampToKafka(true) // 给每条记录设设置时间戳
    myProducer.setLogFailuresOnly(false) // 设置是否在 Producer 发生异常时仅仅记录日志
    myProducer.setTransactionalIdPrefix("lb") // 设置自定义的 transactional.id 前缀
    myProducer.ignoreFailuresAfterTransactionTimeout() // 在恢复时忽略事务超时异常

    kafka_data.addSink(myProducer)

    env.execute()
  }
}
