package com.atguigu.scala.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 17:08
 */
object Sample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Sample").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(20, 5, 10, 71, 1, 31, 18, 3, 4, 14)
    val rdd1 = context.parallelize(list1)

    //抽样个数在总个数的10%左右
    //第一个参数withReplacement  代表抽中的元素是否放回
    val rdd2 = rdd1.sample(false, 0.1)
    rdd2.collect.foreach(println)

    context.stop()
  }
}
