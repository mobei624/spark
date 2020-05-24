package com.atguigu.scala.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-10 13:46
 */
object Cogroup {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Join").setMaster("local[2]")
    val context = new SparkContext(conf)
    var rdd1 = context.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "d")))
    var rdd2 = context.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc"), (2, "dd")))


    val rdd3 = rdd1.cogroup(rdd2)


    rdd3.collect.foreach(println)
    context.stop()
  }

}
