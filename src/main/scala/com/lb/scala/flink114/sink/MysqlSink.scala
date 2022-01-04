package com.lb.scala.flink114.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * @ClassName MysqlSink
  * @Description @TODO
  * @Author liuBing
  * @Date 2021/11/24 16:38
  * @Version 1.0
  **/
object MysqlSink extends RichSinkFunction[String] {

  /**
   * 初始化信息
   * @param conf
   */
  override def open(conf: Configuration) = {

  }


  override def invoke(value: String,  context: SinkFunction.Context): Unit = {

  }


  private def saveDateBatch(): Unit = {

  }


}
