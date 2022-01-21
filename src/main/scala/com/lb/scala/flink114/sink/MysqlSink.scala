package com.lb.scala.flink114.sink

import com.lb.unicron.scala.fileutil.ConfigFiles
import com.lb.unicron.scala.jdbc.{DBConnectionInfo, MysqlDao}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.collection.JavaConversions._

/**
 * @ClassName MysqlSink
 * @Description @TODO
 * @Author liuBing
 * @Date 2021/11/24 16:38
 * @Version 1.0
 * */
object MysqlSink extends RichSinkFunction[String] {


  var dao: MysqlDao = _
  var list: java.util.ArrayList[String] = null

  /**
   * 初始化信息
   *
   * @param conf
   */
  override def open(conf: Configuration) = {
    // 初始化数据库
    // val properties = new ConfigFiles("jdbc.properties").getProperties.get
    // val conn = new DBConnectionInfo(properties)
    // dao = new MysqlDao(conn)
  }


  override def invoke(value: String, context: Context): Unit = {
    // dao.saveDataBatch()
    println("===========" + value)
    list.add(value)
    if (list.size() > 100) {
      saveDateBatch(list)
      list.clear()
    }
  }

  private def saveDateBatch(list: java.util.ArrayList[String]): Unit = {
    list.foreach(println)
    println("-----------------" + list.size())
  }

  override def close() {
  }


}
