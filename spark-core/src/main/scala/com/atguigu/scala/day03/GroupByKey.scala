package com.atguigu.scala.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-10 12:39
 */
object GroupByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GroupByKey").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(20, 5, 10, 71, 1, 31, 18)
    val rdd1 = context
      .parallelize(Array("hello", "world", "atguigu", "hello", "hello", "atguigu"))
      .map((_, 1))

    val rdd2 = rdd1.groupByKey()

    context.stop()
  }

}
