package com.lb.scala.flink114.utils

import java.util.Properties

import com.lb.unicron.java.fileutil.ConfigFiles

/**
 * @ClassName ConfigProperties
 * @Description @TODO
 * @Author liubing
 * @Date 2021/11/22 09:06
 * @Version 1.0
 * */
object ConfigProperties {

  val conf: ConfigFiles = new ConfigFiles()


  def getKafkaProperties(groupid: String, offsetreset: String = "earliest"): Properties = {

    val c = conf.getProperties("config.properties")

    // kafka配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", c.getProperty("bootstrap.servers", "127.0.0.1:9092"))
    // properties.setProperty("zookeeper.connect", c.getProperty("zookeeper.connect", "127.0.0.1:2181"))
    properties.setProperty("key.deserializer", c.getProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"))
    properties.setProperty("value.deserializer", c.getProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"))

    properties.setProperty("auto.offset.reset", offsetreset)
    properties.setProperty("group.id", groupid)

    properties
  }

  def getMysqlProperties(): Properties = {
    null
  }


}
