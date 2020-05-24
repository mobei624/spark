package com.atguigu.scala.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 23:09
 *     1. 如果分区内和分区间的聚合逻辑不一样, 用 aggregateByKey
 *     2. 如果分区内和分区间逻辑一样  reduceByKey
 *     3. 如果是聚合应用使用reduceByKey, 因为他有预聚合, 可以提高性能
 *     4. 如果分组的目的不是为了聚合, 这个时候就应该使用groupByKey
 */
object ReduceByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ReduceByKey").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(("hello", 1), ("hello", 2), ("world", 2), ("world", 2), ("hello", 2))
    val rdd1 = context.parallelize(list1)

//    val rdd2 = rdd1.reduceByKey(_ + _)

    val rdd3 = context.parallelize(Array("hello", "world", "atguigu", "hello", "hello", "atguigu"))
    val rdd2 = rdd3.map((_, 1))
      .reduceByKey(_ + _)

    rdd2.collect.foreach(println)

    context.stop()
  }

}
