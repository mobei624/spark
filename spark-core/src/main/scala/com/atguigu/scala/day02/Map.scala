package com.atguigu.scala.day02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author mobei
 * @create 2020-05-07 13:06
 */
object Map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Map").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(20, 50, 100, 70, 10, 30, 80)
    val rdd1 = context.parallelize(list1)

    //    val rdd2 = rdd1.map(_ * 2)

    //mapPartitions 对每个分区做一次map操作
        val rdd2 = rdd1.mapPartitions(it =>
    //      println(it.toList)
    //      it
          it.map(_ + 1)
        )

    //多一个分区的索引
    val rdd3 = rdd1.mapPartitionsWithIndex((index, it) => it.map((index, _)))

    rdd3.collect.foreach(println)

    context.stop()


  }

}
