package com.lb.scala.flink114.datastream

import com.lb.scala.flink114.sink.MysqlSink
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._

import java.util.Properties

/**
 * @ClassName Kafka2Mysql
 * @Description @TODO
 * @Author liubing
 * @Date 2021/11/24 16:37
 * @Version 1.0
 */
object Kafka2Mysql extends App {

  val log = LoggerFactory.getLogger(Kafka2LocalFile.getClass)

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // val env = StreamExecutionEnvironment.createLocalEnvironment(1)

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("group.id", "test")

  //构建FlinkKafkaConsumer
  val myConsumer = new FlinkKafkaConsumer[String](
    "lb3",
    new SimpleStringSchema(),
    // ConfigProperties.getKafkaProperties("lb3")
    properties
  )

  //指定偏移量
  // myConsumer.setStartFromEarliest()      // 尽可能从最早的记录开始
  // myConsumer.setStartFromLatest()        // 从最新的记录开始
  // myConsumer.setStartFromTimestamp(111)  // 从指定的时间开始（毫秒）
  myConsumer.setStartFromGroupOffsets()     // 默认的方法

  val kafka_data_source = env.addSource(myConsumer)
  env.enableCheckpointing(50000)

  // kafka_data_source.map(x => println(x + "------------------"))
  kafka_data_source.print()
  kafka_data_source.addSink(MysqlSink)

  env.execute()
}