package com.atguigu.sparkstreaming.day10

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author mobei
 * @create 2020-05-18 17:11
 *
 *        kafka直联模式
 *        kafka-0-08_2.11
 */
/*object DirectApi {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("DirectApi")
    val context = new StreamingContext(conf,Seconds(3))

    val param = Map[String, String](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "group.id" -> "atguigu1"
    )

    val sourceStream: InputDStream[(String, String)] = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](
      context,
      param,
      Set("spark1128")
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
