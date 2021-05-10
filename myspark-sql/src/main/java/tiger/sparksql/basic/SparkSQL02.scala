package tiger.sparksql.basic

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 *
 * @Author Zenghu
 * @Date 2021/5/5 16:34
 * @Description
 * @Version: 1.0
 *
 * */
object SparkSQL02 {

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


    // TODO RDD -> DataFrame

    val rdd0: RDD[Int] = spark.sparkContext.makeRDD(List(1, 2, 3, 4, 5))

    import spark.implicits._
    val df1 = rdd0.toDF
    //df1.printSchema()
    //df1.show()


    // TODO DataFrame -> DataSet -> RDD

    val ds02: Dataset[Row] = df.as("user")
    val rdd02 = ds02.rdd
    //rdd02.foreach(println)

   // TODO DataFrame -> DataSet

    val ds03 = df.as("user2")
    val res03 = ds03.select("name")
    // res03.show()


    // TODO DataSet -> DataFrame

    val df04 = ds03.toDF()
    // df04.show()



    // TODO DataSet -> RDD
    val rdd04 = ds03.rdd



    // TODO RDD -> DataSet

    val ds05 = rdd0.toDS()








    // TODO 关闭SparkSession
    spark.close()


  }


}
