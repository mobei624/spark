package com.atguigu.sparksql.day08

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * @author mobei
 * @create 2020-05-14 22:36
 *
 *        自定义聚合函数
 */
object UDAFDemo{
  def main(args: Array[String]): Unit = {


  }

}


class myUDAF extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = ???

  override def bufferSchema: StructType = ???

  override def dataType: DataType = ???

  override def deterministic: Boolean = ???

  override def initialize(buffer: MutableAggregationBuffer): Unit = ???

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def evaluate(buffer: Row): Any = ???
}