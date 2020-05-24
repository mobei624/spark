package com.atguigu.scala.day02

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
 * @author mobei
 * @create 2020-05-07 18:13
 */
object SortBy {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SortBy").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(20, 5, 10, 71, 1, 31, 18)
    val rdd1 = context.parallelize(list1)

    //默认升序排
    val rdd2 = rdd1.sortBy(x => x)
    val rdd3 = rdd1.sortBy(x => x, false)
    rdd2.collect.foreach(println)
    println("--------------")
    rdd3.collect.foreach(println)

    println("--------------")

    //长度升序，字母表降序
    val list2 = List("aab", "abc", "dd", "c", "defc")
    val rdd4 = context.parallelize(list2)
    val rdd5 = rdd4.sortBy(x => (x.length, x))(Ordering Tuple2(Ordering.Int, Ordering.String.reverse),
      ClassTag(classOf[(Int, String)]))
    rdd5.collect.foreach(println)

    context.stop()
  }

}
