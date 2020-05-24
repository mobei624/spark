package com.atguigu.scala.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 16:44
 */
object GroupBy {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GroupBy").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(2, 5, 10, 7, 1, 3, 8)

    val rdd1 = context.parallelize(list1)
/*    //groupBy  需要shuffle
    val rdd2 = rdd1.groupBy(x => x % 2)

    rdd2.collect.foreach(println)


    //val rdd3 = rdd2.mapValues(_.sum)

    val rdd3 = rdd2.map({
      case (k, it) => (k, it.sum)
    })

    println("------------")
    rdd3.collect.foreach(println)*/


    //groupBy去重
    val list = List(20, 5, 10, 71, 1, 31, 18, 20, 10, 31, 31)
    val rdd = context.parallelize(list)
    val rdd4 = rdd.groupBy(x => x).map(_._1)

    println(rdd4.collect.mkString("-"))

    context.stop()
  }

}
