package tiger.sparkcore.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Author Zenghu
  * @Date 2021/4/10 22:40
  * @Description
  * @Version: 1.0
  *
  **/
object SparkTransform_glom {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")

    val sc = new SparkContext(conf)

    val rdd0: RDD[Int] = sc.makeRDD(List(1,2,3,4,5), 3)

    // 将同一分区的数据转成数组
    val rdd1: RDD[Array[Int]] = rdd0.glom()

    rdd1.collect().foreach(data=>println(data.mkString(",")))


    sc.stop()

  }

}
