package com.atguigu.sparksql.day09

import org.apache.spark.sql.SparkSession

/**
 * @author mobei
 * @create 2020-05-15 14:04
 */
object HiveWrite {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("HiveWrite")
      .enableHiveSupport()
      // 配置仓库的地址
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .getOrCreate()
    spark.sql("use test1128")
    // 1. 先有df
    val df = spark.read.json("E:\\person.json")
    //        val df = List(("a", 11L), ("b", 22L)).toDF("n", "a")
    df.printSchema()

    /*
    saveAsTable，只需要原表和导入数据的列名一样即可，列的顺序无需一致
    insertInto，原表和导入数据的列名不重要，但是导入数据的字段类型顺序需要和原表一致

    即如果原表字段顺序是(name:String, age:Long)
    saveAsTable 导入的数据可以是(age:13,name:"aa")
    insertInto  导入的数据可以是(n:"abc",ag:15)

    */

    // 2. 写法1: 使用saveAsTable
//    df.write.saveAsTable("user_1")
    //追加
//    df.write.mode("append").saveAsTable("user_1")

    //写法2   等价于df.write.mode("append").saveAsTable("user_1")
//    df.write.insertInto("user_1")

    //写法3，使用hive的sql语句
    spark.sql("insert into table user_1 values(24,'male','zengk')")

    //    spark.sql("drop database test1128").show
    spark.sql("select * from user_1").show
    spark.close()
  }

}
