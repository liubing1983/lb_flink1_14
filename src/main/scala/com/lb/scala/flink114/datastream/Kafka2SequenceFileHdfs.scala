package com.lb.scala.flink114.datastream

import com.lb.scala.flink114.utils.ConfigProperties
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.sequencefile.SequenceFileWriterFactory
import org.apache.flink.runtime.util.HadoopUtils
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._


/**
  * @ClassName Kafka2SequenceFileHdfs
  * @Description @TODO
  * @Author liubing
  * @Date 2021/11/22 16:24
  * @Version 1.0
  **/
object Kafka2SequenceFileHdfs {


  val log = LoggerFactory.getLogger(Kafka2SequenceFileHdfs.getClass)
  val env = StreamExecutionEnvironment.createLocalEnvironment(4)


  def main(args: Array[String]): Unit = {

    //构建FlinkKafkaConsumer
    val myConsumer = new FlinkKafkaConsumer[String]("lb2", new SimpleStringSchema(), ConfigProperties.getKafkaProperties("qwer9"))

    //指定偏移量
    // myConsumer.setStartFromEarliest()      // 尽可能从最早的记录开始
    // myConsumer.setStartFromLatest()        // 从最新的记录开始
    // myConsumer.setStartFromTimestamp(111)  // 从指定的时间开始（毫秒）
    // myConsumer.setStartFromGroupOffsets()  // 默认的方法

    val kadka_data = env.addSource(myConsumer)


    env.enableCheckpointing(5000)
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)

    val config = OutputFileConfig
      .builder()
      .withPartPrefix("prefix")
      .withPartSuffix(".ext")
      .build()

    val hadoopConf: Configuration = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration())
    val outPath = new Path("hdfs://cdh1:8020/user/liubing/flink/kafka2hdfs")
    val sf: SequenceFileWriterFactory[NullWritable, Text] =
      new SequenceFileWriterFactory(hadoopConf, classOf[NullWritable], classOf[Text])

    // forBulkFormat  按列写入
    // forRowFormat   按行写入
//    val sink: StreamingFileSink[Tuple2[LongWritable, Text]] = StreamingFileSink
//      .forBulkFormat(outPath, sf)
//      .build()


    kadka_data.print()

    // kadka_data.addSink(sink).name("save hdfs")

    env.execute("Kafka 2 HDFS FlinkDemo")
  }

}
