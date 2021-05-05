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
object SparkTransform_mapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")

    val sc = new SparkContext(conf)

    // 分区划分：
    // 元素个数：5
    // 分区数：3
    // 分区0： [0*5/3, 1*5/3)  向下取整
    // 分区1： [1*5/3, 2*5/3)
    // 分区2： [2*5/3, 3*5/3)
    val rdd0: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3)

    val rdd1 = rdd0.mapPartitionsWithIndex((index, datas) => {
      datas.map(data => (index, data))
    })

    rdd1.collect().foreach(println(_))


    sc.stop()

  }

}
