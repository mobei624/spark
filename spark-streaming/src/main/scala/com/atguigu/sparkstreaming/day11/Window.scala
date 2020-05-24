package com.atguigu.sparkstreaming.day11

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author mobei
 * @create 2020-05-19 21:52
 */
object Window {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("Window")
    val context = new StreamingContext(conf,Seconds(3))

    val stream = context
      .socketTextStream("hadoop102", 9999)
      //直接对流先做一个窗口操作，则后面的所有操作都是基于窗口
      //省却了后面的麻烦，但是无法避免重复计算
      .window(Seconds(9))

    val result = stream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    result.print

    context.start()
    context.awaitTermination()
  }

}
