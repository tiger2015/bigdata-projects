package tiger.sparksql.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 *
 * @Author Zenghu
 * @Date 2021/5/9 11:16
 * @Description
 * @Version: 1.0
 *
 * */
object SparkHive01 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("spark-hive")

    // 开启支持HIVE
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()


    // spark.sql("show databases").show()

    spark.sql("use test")

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS  `user_visit_action`(
        |`date` string,
        |`user_id` bigint,
        |`session_id` string,
        |`page_id` bigint,
        |`action_time` string,
        |`search_keyword` string,
        |`click_category_id` bigint,
        |`click_product_id` bigint,
        |`order_category_ids` string,
        |`order_product_ids` string,
        |`pay_category_ids` string,
        |`pay_product_ids` string,
        |`city_id` bigint)
        |row format delimited fields terminated by '\t';
        |""".stripMargin)


    spark.sql(
      """
        |load data local inpath 'data/user_visit_action.txt' into table test.user_visit_action
        |""".stripMargin)



    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS  `product_info`(
        |`product_id` bigint,
        |`product_name` string,
        |`extend_info` string)
        |row format delimited fields terminated by '\t';
        |
        |""".stripMargin)


    spark.sql(
      """
        |load data local inpath 'data/product_info.txt' into table test.product_info;
        |""".stripMargin)



    spark.sql(
      """
        |
        |CREATE TABLE IF NOT EXISTS  `city_info`(
        |`city_id` bigint,
        |`city_name` string,
        |`area` string)
        |row format delimited fields terminated by '\t';
        |
        |""".stripMargin)


    spark.sql(
      """
        |load data local inpath 'data/city_info.txt' into table test.city_info;
        |""".stripMargin)


    spark.sql("show tables").show()


    spark.close()

  }



}
