package com.atguigu.sparkstreaming.day10

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.{immutable, mutable}

/**
 * @author mobei
 * @create 2020-05-17 21:30
 */
object RddQueue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("RddQueue")
    val context = new StreamingContext(conf,Seconds(3))

    val queue = mutable.Queue[RDD[Int]]()
    val stream = context.queueStream(queue, true)

    val result = stream.reduce(_ + _)
    result.print
    context.start()

    while (true){
      val rdd = context.sparkContext.parallelize(1 to 100)
      queue.enqueue(rdd)
      Thread.sleep(100)
    }
    context.awaitTermination()
  }

}
