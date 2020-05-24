package com.atguigu.scala.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-04 17:24
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val context = new SparkContext(conf)
    val line:RDD[String] = context.textFile(args(0))

    val wordCountRDD  = line
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)


    val array = wordCountRDD.collect
    array.foreach(println)
    context.stop()

  }

}
