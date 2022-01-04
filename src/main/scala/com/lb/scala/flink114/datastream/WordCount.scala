package com.lb.scala.flink114.datastream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.execution.JobClient
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

/**
  * @ClassName WordCount
  * @Description @TODO 使用StreamExecutionEnvironment执行BATCH任务
  * @Author liubing
  * @Date 2021/11/15 17:01
  * @Version 1.0
  **/
object WordCount {

  val log = LoggerFactory.getLogger(WordCount.getClass)

  def main(args: Array[String]): Unit = {

    // 创建一个执行环境，表示当前执行程序的上下文。
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 返回集群执行环境，将Jar提交到远程服务器。需要在调用时指定JobManager的IP和端口号，并指定要在集群中运行的Jar包。
    // val env = StreamExecutionEnvironment.createRemoteEnvironment("cdh1", 8081, "/Users/liubing/IdeaProjects/decepticon_2.11/flink114/target/flink114-1.0-SNAPSHOT.jar")

    // 设置任务为批处理
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)

    val textFile = env.readTextFile("hdfs://cdh1:8020/user/hive/warehouse/a.txt")
    textFile.flatMap(_.split(":", -1))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .print()

    val path = new Path("hdfs://cdh1:8020/user/hive/warehouse/a.txt")
    val textInputFormat = new TextInputFormat(path)
    textInputFormat.setFilesFilter(FilePathFilter.createDefaultFilter())

    val file = env.readFile[String](textInputFormat, "hdfs://cdh1:8020/user/hive/warehouse/a.txt")
    file.flatMap(_.split(",", -1))
      .map((_, 1))
      .print()

    // execute() 方法将等待作业完成，然后返回一个 JobExecutionResult，其中包含执行时间和累加器结果
    // BATCH模式时执行execute会抛错
    // val jobExecutionResult : JobExecutionResult =  env.execute("123")
    // jobExecutionResult.getNetRuntime

    // 异步得到运行结果
    val jobClient: JobClient = env.executeAsync()
    println(s"getJobID: ${jobClient.getJobID.toString}")
  }
}
