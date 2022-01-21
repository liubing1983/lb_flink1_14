package com.lb.scala.flink114.cdc


import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._

object MysqlToMysqlTableApi extends App {


  // 环境配置
//  val settings = EnvironmentSettings
//    .newInstance()
//    .inStreamingMode()
//    .build()

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  //val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

  //val c = conf.getProperties("cdc/MysqlToMysqlTableApi.properties")

  //val sourceDDL = c.getProperty("sourceDDL", "")

  //val sinkDDL = c.getProperty("sinkDDL", "")

  //tableEnv.executeSql(sourceDDL)

  //tableEnv.executeSql(sinkDDL)
}
