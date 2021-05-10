package tiger.sparksql.basic

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @Author Zenghu
 * @Date 2021/5/5 16:34
 * @Description
 * @Version: 1.0
 *
 * */
object SparkSQL_UDF01 {

  def main(args: Array[String]): Unit = {


    val config = new SparkConf()
    config.setMaster("local[*]")
    config.setAppName("SparkSQL")
    // TODO 创建SparkSession
    val spark = SparkSession.builder().config(config).getOrCreate()

    // TODO 读取json格式的数据
    val df: DataFrame = spark.read.json("data/user.json")
    //df.show()

    // TODO 创建视图
    df.createOrReplaceTempView("user")


    // 注册自定函数
    spark.udf.register("prefix", (name: String) => name + "_Name")
    spark.udf.register("add_age", (age: Int) => age + 1)

    val res = spark.sql("select prefix(name), add_age(age) from user")

    res.show()



    // TODO 关闭SparkSession
    spark.close()


  }


}
