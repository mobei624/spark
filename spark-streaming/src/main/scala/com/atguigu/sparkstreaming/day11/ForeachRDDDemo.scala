package com.atguigu.sparkstreaming.day11

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author mobei
 * @create 2020-05-18 14:02
 */
object ForeachRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("ForeachRDDDemo")
    val context = new StreamingContext(conf,Seconds(3))
    val lineStream: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 9999)

    val wordCountStream = lineStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    context.start()
    context.awaitTermination()
  }

}
