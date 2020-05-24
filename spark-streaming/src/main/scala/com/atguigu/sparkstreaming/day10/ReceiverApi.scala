package com.atguigu.sparkstreaming.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author mobei
 * @create 2020-05-18 16:42
 *
 *         Kafka Receive模式
 *         kafka-0-08_2.11
 */
/*object ReceiverApi {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ReceiverApi")
    val context = new StreamingContext(conf, Seconds(3))

    val sourceStream: ReceiverInputDStream[(String, String)] =
      KafkaUtils.createStream(
        context,
        "hadoop102:2181,hadoop103:2181,hadoop104:2181/mykafka",
        "atguigu",
        Map("spark1128" -> 1)
      )
    sourceStream
      .map(_._2)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print

    context.start()
    context.awaitTermination()
  }

}*/
