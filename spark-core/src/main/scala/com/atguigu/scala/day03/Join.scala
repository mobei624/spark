package com.atguigu.scala.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-10 13:32
 */
object Join {
  def main(args: Array[String]): Unit = {

    //join 只对kv形式的RDD使用，连接条件是key相等
    //内连接、外连接
    val conf = new SparkConf().setAppName("Join").setMaster("local[2]")
    val context = new SparkContext(conf)
    var rdd1 = context.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "d")))
    var rdd2 = context.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc"), (2, "dd")))

    //内连接
    //    val rdd3 = rdd1.join(rdd2)
    //左外连接, 左边都有，对应右边没有的用None
    //    val rdd3 = rdd1.leftOuterJoin(rdd2)
    //右外连接, 右边都有，对应左边没有的用None
    //    val rdd3 = rdd1.rightOuterJoin(rdd2)
    //满外连接
    val rdd3 = rdd1.fullOuterJoin(rdd2)


    rdd3.collect.foreach(println)
    context.stop()
  }

}
