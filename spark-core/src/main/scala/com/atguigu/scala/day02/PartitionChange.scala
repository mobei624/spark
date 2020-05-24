package com.atguigu.scala.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 17:47
 */
object PartitionChange {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PartitionChange").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(20, 5, 10, 71, 1, 31, 18)
    val rdd1 = context.parallelize(list1,4)

    //coalesce 改变分区数 ，变成numPartitions个
    //coalesce 默认参数时可以减少分区数，但不能增加分区数
    //coalesce默认不经过shuffle, 改变第三个参数为true可以增加分区
    val rdd2 = rdd1.coalesce(3)
    println("rdd1===" + rdd1.getNumPartitions)
    println("rdd2===" + rdd2.getNumPartitions)

    val rdd3 = rdd1.coalesce(5, true)
    println("rdd3===" + rdd3.getNumPartitions)


    //repartition 实际就是使用coalesce，一般用于增加分区
    val rdd4 = rdd1.repartition(5)
    println("rdd4===" + rdd4.getNumPartitions)

    context.stop()
  }

}
