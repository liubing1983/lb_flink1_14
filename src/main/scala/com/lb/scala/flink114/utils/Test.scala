package com.lb.scala.flink114.utils

/**
 * @ClassName Test
 * @Description @TODO
 * @Author liubing
 * @Date 2021/11/16 12:51
 * @Version 1.0
 * */
object Test extends App {

  val a = "10"
  val b = ""
  var c = 0

  try {
    c = a.toInt
  } catch {
    case e: NumberFormatException => c = 0
  }

  println(c)

}
