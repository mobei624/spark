package com.atguigu.sparksql.practice

import java.text.DecimalFormat

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

/**
 * @author mobei
 * @create 2020-05-15 20:19
 */
object AreaClickApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("AreaClickApp")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    spark.udf.register("remark", new CityRemarkUDAF)

    spark.sql("use test1128")
    spark.sql(
      """select
        |  ci.area,
        |  ci.city_name,
        |  pi.product_name,
        |  uvi.click_product_id
        |from user_visit_action uvi
        |join product_info pi on uvi.click_product_id=pi.product_id
        |join city_info ci on uvi.city_id=ci.city_id""".stripMargin)
      .createOrReplaceTempView("t1")

    spark.sql(
      """select
        |  area,
        |  product_name,
        |  count(*) count,
        |  remark(city_name) remark
        |from t1
        |group by area,product_name""".stripMargin)
      .createOrReplaceTempView("t2")

    spark.sql(
      """select
        |  area,
        |  product_name,
        |  count,
        |  remark,
        |  rank() over(partition by area order by count desc) rk
        |from t2""".stripMargin)
      .createOrReplaceTempView("t3")

    spark.sql(
      """select
        |  area,
        |  product_name,
        |  count,
        |  remark
        |from t3
        |where rk<=3""".stripMargin)
      .coalesce(1)
      .write
      .mode("overwrite")
      .saveAsTable("result")

    spark.close
  }

}


/*
class CityRemarkUDAF extends UserDefinedAggregateFunction {

  //输入数据类型  string   city_name
  override def inputSchema: StructType = StructType(StructField("city",StringType)::Nil)

  override def bufferSchema: StructType =
    StructType(StructField("map", MapType(StringType, LongType)) :: StructField("total", LongType) :: Nil)

  //输出数据类型 string
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  //初始化, 对缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }

  //分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    input match{
        //传入一个城市
      case Row(cityName:String) =>
        //更新总数
        buffer(1) = buffer.getLong(1) + 1L
        //更新map
        val map = buffer.getMap[String,Long](0)
        buffer(0) = map + (cityName -> (map.getOrElse(cityName,0L) + 1L))
      case _ =>
    }
  }

  //分区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getMap[String,Long](0)
    val total1 = buffer1.getLong(1)

    val map2 = buffer1.getMap[String,Long](0)
    val total2 = buffer1.getLong(1)

    //合并总数
    buffer1(1) = total1 + total2

    //合并map
    buffer1(0) = map1.foldLeft(map2){
      case (map,(city, count)) =>
        map + (city -> (map.getOrElse(city,0L) + count))
    }
  }

  //返回最终结果
  override def evaluate(buffer: Row): Any = {
    val cityMap = buffer.getMap[String,Long](0)
    val total = buffer.getLong(1)

    val cityTop2: List[(String, Long)] = cityMap.toList.sortBy(-_._2).take(2)
    println(cityTop2)
    val cityRemarkTop2: List[CityRemark] = cityTop2.map {
      case (city, count) => CityRemark(city, count.toDouble / total)
    }
    val cityRemark: List[CityRemark] = cityRemarkTop2 :+ CityRemark("其他",cityRemarkTop2.foldLeft(1D)(_ - _.rate))
    cityRemark.mkString(",")
  }
}



case class CityRemark(city:String,rate:Double){
  val f = new DecimalFormat(".00%")

  // 北京21.20%
  override def toString: String = s"$city:${f.format(rate)}"
}
*/

class CityRemarkUDAF extends UserDefinedAggregateFunction {
  // 输入数据的类型    城市名   StringType
  override def inputSchema: StructType = StructType(StructField("city", StringType) :: Nil)

  // 北京->1000 天津->10  Map 缓冲区   MapType(key类型, value类型)
  // 最好再缓冲一个总数
  override def bufferSchema: StructType =
    StructType(StructField("map", MapType(StringType, LongType)) :: StructField("total", LongType) :: Nil)

  // 输出类型  StringType
  override def dataType: DataType = StringType

  // 确定性
  override def deterministic: Boolean = true

  // 初始化  对缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }


  // 分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    input match {
      // 传入了一个城市
      case Row(cityName: String) =>
        // 北京
        // 1. 先更新总数
        buffer(1) = buffer.getLong(1) + 1L
        // 2. 再去更新具体城市的数量
        val map = buffer.getMap[String, Long](0)
        buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
      // 传入了一个null
      case _ =>
    }
  }

  // 分区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 把数据合并, 再放入到buffer1中
    val map1 = buffer1.getMap[String, Long](0)
    val total1 = buffer1.getLong(1)

    val map2 = buffer2.getMap[String, Long](0)
    val total2 = buffer2.getLong(1)

    // 1. 先合并总数
    buffer1(1) = total1 + total2
    // 2. 合并map
    buffer1(0) = map1.foldLeft(map2) {
      case (map, (city, count)) =>
        map + (city -> (map.getOrElse(city, 0L) + count))
    }
  }

  // 返回最终的值
  override def evaluate(buffer: Row): Any = {
    val cityCount = buffer.getMap[String, Long](0)
    val total = buffer.getLong(1)

    // 北京21.2%，天津13.2%，其他65.6%   排序取前2
    val cityCountTop2: List[(String, Long)] = cityCount.toList.sortBy(-_._2).take(2)

    val cityRemarkTop2: List[CityRemark] = cityCountTop2.map {
      case (city, count) => CityRemark(city, count.toDouble / total)
    }
    // top2 + 其他
    val cityRemarks = cityRemarkTop2 :+ CityRemark("其他", cityRemarkTop2.foldLeft(1D)(_ - _.rate))

    cityRemarks.mkString(", ")
  }
}

case class CityRemark(city: String, rate: Double) {
  val f = new DecimalFormat(".00%")

  // 北京21.20%
  override def toString: String = s"$city:${f.format(rate)}"
}

/*

地区	商品名称		点击次数	城市备注
华北	商品A		100000	北京21.2%，天津13.2%，其他65.6%
华北	商品P		80200	北京63.0%，太原10%，其他27.0%
华北	商品M		40000	北京63.0%，太原10%，其他27.0%


1.	查询出来所有的点击记录, 并与 city_info 表连接, 得到每个城市所在的地区. 与 Product_info 表连接得到产品名称
2.	按照地区和商品 id 分组, 统计出每个商品在每个地区的总点击次数
3.	每个地区内按照点击次数降序排列
4.	只取前三名. 并把结果保存在数据库中
5.	城市备注需要自定义 UDAF 函数

//1  t1
select
  ci.area,
  ci.city_name,
  pi.product_name,
  uvi.click_category_id
from user_visit_action uvi
join product_info pi on uvi.click_product_id=pi.product_id
join city_info ci on uvi.city_id=ci.city_id

//2   t2
select
  area,
  product_name,
  count(*) count
from t1
group by area,product_name

//3   t3
select
  area,
  product_name,
  count,
  rank() over(partition by area order by count desc) rk
from t2

//4
select
  area,
  product_name,
  count
from t3
where rk<=3



建表：
CREATE TABLE `user_visit_action`(
  `date` string,
  `user_id` bigint,
  `session_id` string,
  `page_id` bigint,
  `action_time` string,
  `search_keyword` string,
  `click_category_id` bigint,
  `click_product_id` bigint,
  `order_category_ids` string,
  `order_product_ids` string,
  `pay_category_ids` string,
  `pay_product_ids` string,
  `city_id` bigint)
row format delimited fields terminated by '\t';
load data local inpath '/home/atguigu/data/user_visit_action.txt' into table user_visit_action;

CREATE TABLE `product_info`(
  `product_id` bigint,
  `product_name` string,
  `extend_info` string)
row format delimited fields terminated by '\t';
load data local inpath '/home/atguigu/data/product_info.txt' into table product_info;

CREATE TABLE `city_info`(
  `city_id` bigint,
  `city_name` string,
  `area` string)
row format delimited fields terminated by '\t';
load data local inpath '/home/atguigu/data/city_info.txt' into table city_info;


* */
