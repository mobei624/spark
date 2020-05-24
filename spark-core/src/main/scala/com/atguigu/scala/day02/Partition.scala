package com.atguigu.scala.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 14:12
 */
object Partition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Partition").setMaster("local[*]")
    val context = new SparkContext(conf)
    val list1 = List(20, 50, 100, 70, 10, 30, 80)
    //默认分区数是 local[]设置的核心数
    //* 时是随机分区
    val rdd1 = context.parallelize(list1)
    val rdd2 = context.parallelize(list1,3)

    val rdd3 = rdd2.mapPartitionsWithIndex((index, it) => it.map((index, _)))
    val rdd4 = rdd1.mapPartitionsWithIndex((index, it) => it.map((index, _)))

    rdd3.collect.foreach(println)
    println("-----------------------")
    rdd4.collect.foreach(println)
    context.stop()
  }
}
