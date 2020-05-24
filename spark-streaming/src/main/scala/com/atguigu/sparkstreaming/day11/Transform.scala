package com.atguigu.sparkstreaming.day11

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author mobei
 * @create 2020-05-19 13:30
 */
object Transform {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("Transform")
    val context = new StreamingContext(conf,Seconds(3))

    val stream: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 9999)
    //把对流的操作转换成对RDD，RDD算子多
    val value: DStream[(String, Int)] = stream.transform(rdd => {
      rdd.flatMap(_.split(" "))
        .map((_, 1))
        .reduceByKey(_ + _)
    })

    value.print

    context.start()
    context.awaitTermination()
  }

}
