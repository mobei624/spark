package com.atguigu.sparkcore.project.bean

/**
 * @author mobei
 * @create 2020-05-11 14:08
 */
case class UserAction(date: String,
                      user_id: Long,
                      session_id: String,
                      page_id: Long,
                      action_time: String,
                      search_keyword: String,
                      click_category_id: Long, //点击的品类id
                      click_product_id: Long,  //点击的产品id
                      order_category_ids: String,  //下单的品类id
                      order_product_ids: String,   //下单的产品id
                      pay_category_ids: String,   //付款的品类id
                      pay_product_ids: String,   //付款的产品id
                      city_id: Long
                     )
