package tiger.sparksql.basic

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 *
 * @Author Zenghu
 * @Date 2021/5/5 16:34
 * @Description
 * @Version: 1.0
 *
 * */
object SparkSQL01 {

  def main(args: Array[String]): Unit = {


    val config = new SparkConf()
    config.setMaster("local[*]")
    config.setAppName("SparkSQL")
    // TODO 创建SparkSession
    val spark = SparkSession.builder().config(config).getOrCreate()










    // TODO 关闭SparkSession
    spark.close()





  }


}
