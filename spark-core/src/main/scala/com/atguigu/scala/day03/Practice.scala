package com.atguigu.scala.day03

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author mobei
 * @create 2020-05-06 14:26
 */
object Practice {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val context = new SparkContext(conf)
    val line:RDD[String] = context.textFile(args(0))

    //按单词个数降序排
    val wordCountRDD  = line
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    val wordCount = wordCountRDD
      .map{
        case (k, v)=>(v, k)
      }
      .sortByKey(false)
      .map{
        case (k, v)=>(v, k)
      }

    wordCount.collect.foreach(println)
  }

}
