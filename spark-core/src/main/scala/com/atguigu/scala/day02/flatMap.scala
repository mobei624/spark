package com.atguigu.scala.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 14:37
 */
object flatMap {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("flatMap").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(20, 50, 100, 70, 10, 30, 80)
    val rdd1 = context.parallelize(list1)
    val rdd2 = rdd1.flatMap(x => List(x * x, x * x * x))

    rdd2.collect.foreach(println)
    context.stop()
  }

}
