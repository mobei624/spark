package com.atguigu.scala.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 23:21
 */
object FoldByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ReduceByKey").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(("hello", 1), ("hello", 2), ("world", 2), ("world", 2), ("hello", 2))
    val rdd1 = context.parallelize(list1)

    //zeroValue 的类型必须和键值对中value的类型一致
    //零值只参与预聚合运算，即只在每个分区内参与一次运算，不同分区合并时不会参与运算
    val rdd2 = rdd1.foldByKey(0)(_ + _)

    rdd2.collect.foreach(println)
  }

}
