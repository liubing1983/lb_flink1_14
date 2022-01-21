package com.lb.scala.flink114.table

import org.apache.derby.iapi.sql.dictionary.TableDescriptor
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Schema, TableEnvironment}


object HelloWorld {

  def main(args: Array[String]) {

    val settings = EnvironmentSettings
      .newInstance()
      //.inStreamingMode()
      .inBatchMode()
      .build()

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env, settings)

    val ds = env.readTextFile("/Users/liubing/demo/flink_source_data.txt")

   // tableEnv.createTable()

    // tableEnv.createTable("SourceTable")
  }

}

case class Test(id: Int, name: String)
