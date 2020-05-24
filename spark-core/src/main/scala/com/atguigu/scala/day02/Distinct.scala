package com.atguigu.scala.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 17:29
 */
object Distinct {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Distinct").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(20, 5, 10, 71, 1, 31, 18, 20, 10, 31, 31)
    val rdd1 = context.parallelize(list1)

    val rdd2 = rdd1.distinct()
    println(rdd2.collect.mkString("-"))

    context.stop()
  }

}
