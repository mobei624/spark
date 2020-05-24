package com.atguigu.scala.day04

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-10 16:04
 */
object Partitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Partitioner").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(20, 5, 10, 71, 1, 31, 18)
    val rdd1 = context.parallelize(list1)

    //默认的分区器是hash分区器 HashPartitioner
    context.stop()
  }

}

class MyPartitioner(num:Int) extends Partitioner {
  //返回分完区之后的分区数
  override def numPartitions: Int = num

  //
  override def getPartition(key: Any): Int = {
    key match {
      case null => 0
      case _ => key.hashCode().abs % numPartitions
    }

  }

  override def hashCode(): Int = num

  override def equals(obj: Any): Boolean = {
    obj match {
      case null => false
      case o: MyPartitioner => num == o.num
      case _ => false
    }
  }
}
