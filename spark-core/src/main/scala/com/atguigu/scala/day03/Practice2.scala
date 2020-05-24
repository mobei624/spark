package com.atguigu.scala.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-06 14:26
 *        1.	数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割。
 *                1516609143867 6 7 64 16
 *                1516609143869 9 4 75 18
 *                1516609143869 1 7 87 12
 *        2.	需求: 统计出每一个省份广告被点击次数的 TOP3
 */
object Practice2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice2").setMaster("local[2]")
    val context = new SparkContext(conf)
    //读取文件，获取每一行
    val line:RDD[String] = context.textFile(args(0))

    //从每一行数据中拿到省份、广告id、点击数  ((pro,ad),1)
    val rdd1 = line.map({
      lines => {
        val arr = lines.split(" ")
        ((arr(1), arr(4)), 1)
      }
    })

    //分组，计算点击总数  ((pro,ad),count)
    val rdd2 = rdd1.reduceByKey(_ + _)

    //调整格式，将省份作为key   (pro,(ad,count))
    val rdd3 = rdd2.map({
      case ((pro, ad), count) => (pro, (ad, count))
    })

    //按照省份分组，计算每个省份中各个广告的总点击数，并按降序排，取到前三
    val rdd4 = rdd3.groupByKey().mapValues({
      value => value.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    })


    rdd4.collect.foreach(println)
    context.stop()
  }

}
