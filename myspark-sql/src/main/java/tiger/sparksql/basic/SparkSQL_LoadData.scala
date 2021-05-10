package tiger.sparksql.basic

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @Author Zenghu
 * @Date 2021/5/7 21:44
 * @Description
 * @Version: 1.0
 *
 * */
object SparkSQL_LoadData {

  def main(args: Array[String]): Unit = {

   val config = new SparkConf()
    config.setMaster("local[*]")
    config.setAppName("jdbc")

    val spark = SparkSession.builder().config(config).getOrCreate()

    val props: Properties = new Properties()
    props.setProperty("user", "test")
    props.setProperty("password", "test")

   // TODO 从mysql数据库中加载数据
   val df: DataFrame = spark.read.jdbc(url = "jdbc:mysql://127.0.0.1:3306/test", "consumption_record", props)

   df.show()

   //spark.read.format("json").load("data/user.json")
   //spark.read.json("data/user.json")

   df.write.format("json").save("data/consumption.json")



    spark.close()


  }
}
