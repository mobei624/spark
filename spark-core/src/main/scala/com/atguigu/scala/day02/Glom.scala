package com.atguigu.scala.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 16:38
 */
object Glom {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Glom").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(20, 50, 100, 70, 10, 30, 80)
    val rdd1 = context.parallelize(list1,4)

    //glom 按分区数获得数组，即有n个分区，那么得到的rdd中就有n个数组
    val rdd2 = rdd1.glom()

    rdd2.map(_.toList).collect.foreach(println)

    context.stop()
  }

}
