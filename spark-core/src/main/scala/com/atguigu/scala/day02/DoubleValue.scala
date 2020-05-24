package com.atguigu.scala.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 19:41
 */
object DoubleValue {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DoubleValue").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(20, 5, 10, 71, 1, 31, 18, 18)
    val list2 = List(2, 5, 10, 7, 1, 17, 18)
    val rdd1 = context.parallelize(list1, 2)
    val rdd2 = context.parallelize(list2, 2)

   /* //并集，分区数相加，不shuffle
    val rdd3 = rdd1 ++ rdd2
    //val rdd3 = rdd1.union(rdd2)
    rdd3.collect.foreach(println)
    println(rdd3.getNumPartitions)*/

    /*//交集 , 分区数等于两个rdd中分区数大的那个
    //交集能去重
    val rdd4 = rdd1.intersection(rdd2)
    rdd4.collect.foreach(println)
    println(rdd4.getNumPartitions)

    println("=================")*/

    /*//差集
    val rdd5 = rdd1.subtract(rdd2)
    rdd5.collect.foreach(println)
    println(rdd5.getNumPartitions)*/


    /*//笛卡尔积，得到元组，两个rdd的数全部配对一次
    val rdd6 = rdd1.cartesian(rdd2)
    rdd6.collect.foreach(println)*/


    //zip
    //分区数不同的两个rdd不能拉
    //每个分区中元素个数不同时不能拉

    /*val rdd7 = rdd1.zip(rdd2)
    rdd7.collect.foreach(println)*/

    //如果想要分区元素不同也能拉，可以使用zipPartitions
    //分区内用scala的算子
    val rdd8 = rdd1.zipPartitions(rdd2)((it1, it2) => {
//      it1.zip(it2)
      it1.zipAll(it2,-1,-2)
    })
    rdd8.collect.foreach(println)

    context.stop()
  }

}
