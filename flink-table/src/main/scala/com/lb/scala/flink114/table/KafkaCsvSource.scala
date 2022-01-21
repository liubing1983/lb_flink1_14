package com.lb.scala.flink114.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object KafkaCsvSource {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val bSettings = EnvironmentSettings.newInstance.build
    val bsTableEnv = StreamTableEnvironment.create( env, bSettings)

    val kafka_order_source =
      """
        |CREATE TABLE KafkaTable (
        |  `user_id` BIGINT,
        |  `item_id` BIGINT
        |) WITH (
        |  'connector.type' = 'kafka',
        |  'topic' = 'lb',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'properties.group.id' = 'testGroup',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'csv'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(kafka_order_source)
    bsTableEnv.executeSql("select * from KafkaTable").print()

    env.execute("123")
  }
}
