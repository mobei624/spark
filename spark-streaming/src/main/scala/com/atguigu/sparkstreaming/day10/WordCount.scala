package com.atguigu.sparkstreaming.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author mobei
 * @create 2020-05-17 20:02
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("WordCount")
    val context = new StreamingContext(conf,Seconds(3))

    val sourceStream = context.socketTextStream("hadoop102", 9999)
    val resultStream = sourceStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    resultStream.print(100)

    context.start()

    context.awaitTermination()
  }

}
