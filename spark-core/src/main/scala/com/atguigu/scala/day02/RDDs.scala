package com.atguigu.scala.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 13:13
 */
object RDDs {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDDs").setMaster("local[2]")
    val context = new SparkContext(conf)

    //从集合中创建 RDD
    val list1 = List(20, 50, 100,70, 10, 30, 80)
    val rdd1 = context.parallelize(list1)

    //从外部存储创建 RDD

    //从其他 RDD 转换得到新的 RDD。

    context.stop()
  }

}
