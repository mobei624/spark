package com.atguigu.scala.day03

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 21:17
 */
object PartitionBy {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PartitionBy").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List("hello", "atguigu", "hello", "world", "hello", "world")
    val rdd1 = context.parallelize(list1, 2).map((_, 1))

    //针对（key, value）元组形式的RDD分区
    //根据key分区，和value无关
    val rdd2 = rdd1.partitionBy(new HashPartitioner(3))
    rdd2.glom().map(_.toList).collect().foreach(println)

    println("=======================")

    //使用value分区：交换key、value，分区，然后再次交换
    val rdd3 = rdd1.map {
      case (k, v) => (v, k)
    }
      .partitionBy(new HashPartitioner(3))
      .map {
        case (k, v) => (v, k)
      }
    rdd3.glom().map(_.toList).collect().foreach(println)


    context.stop()
  }

}
