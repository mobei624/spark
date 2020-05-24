package com.atguigu.sparkcore.project.app

import com.atguigu.sparkcore.project.bean.UserAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-11 13:56
 *
 *         需求1：Top10 热门品类
 *         需求2：Top10热门品类中每个品类的 Top10 活跃 Session 统计
 *         需求3：页面单跳转转化率
 *
 */
object ProjectApp {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ProjectApp").setMaster("local[2]")
    val context = new SparkContext(conf)

    //从文件中读取数据，封装到样例类中
    val sourceRdd = context.textFile("E:\\user_visit_action.txt")

    val userActionRDD: RDD[UserAction] = sourceRdd.map(lines => {
      val strings = lines.split("_")
      UserAction(strings(0),
        strings(1).toLong,
        strings(2),
        strings(3).toLong,
        strings(4),
        strings(5),
        strings(6).toLong,
        strings(7).toLong,
        strings(8),
        strings(9),
        strings(10),
        strings(11),
        strings(12).toLong)
    })
    //需求1 Top10 热门品类
//    val categoryCountList = CategoryTopApp.caclCategoryTopN(context, userActionRDD)

    //需求2
//    CategorySessionTopApp.caclCategoryTopNSession(context, categoryCountList, userActionRDD)

    //需求3  求("1, 2, 3, 4, 5, 6, 7")这个字符串来编号的页面的单跳转转化率
    PageConversionApp.calcPageConversion(context, userActionRDD, "1,2,3,4,5,6,7")
    context.stop()
  }

}
