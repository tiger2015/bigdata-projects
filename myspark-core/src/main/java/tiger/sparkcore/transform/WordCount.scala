package tiger.sparkcore.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @Author Zenghu
 * @Date 2021/3/29 21:13
 * @Description
 * @Version: 1.0
 *
 * */
// 注意实现序列化，否则自定义函数无法使用
object WordCount extends Serializable {


  def main(args: Array[String]): Unit = {

    val config = new SparkConf
    config.setMaster("local[*]")
    config.setAppName("word-count")

    val sparkContext = new SparkContext(config)
    //sparkContext.setLogLevel("DEBUG")

    //val rdd0 = sparkContext.textFile("file:///C:\\Users\\ZengHu\\Desktop\\wordcount.txt")
    val rdd0 = sparkContext.textFile("hdfs://node01:9000/data02")


    val rdd1 = rdd0.flatMap(split(_))

    // 注意：s => (s, 1) 不能写成：_ => (_, 1)否则返回的是匿名函数而不是元组
    val rdd2 = rdd1.map(s => (s, 1))

    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey((v1, v2) =>  v1 + v2 )


    val ret: Array[(String, Int)] = rdd3.collect()

    ret.foreach(println)

    sparkContext.stop()
  }


  def split(line: String): Array[String] = {
    line.split("[.,:;\\s')(]+").map(word => word.toLowerCase)
  }

  def myMap(s: String): (String, Int) = {
    (s, 1)
  }

}
