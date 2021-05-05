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
object SparkTransform_combineByKey {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")

    val sc = new SparkContext(conf)

    val rdd0 = sc.makeRDD(List(("a", 1), ("b", 3), ("c", 4), ("a", 2), ("c", 6)), 2)

    // 第一个是计数，第二个是总数
    val rdd1: RDD[(String, (Int, Int))] = rdd0.combineByKey((1, _)
      , (c: (Int, Int), v) => {
        (c._1 + 1, c._2 + v)
      }, (v1: (Int, Int), v2: (Int, Int)) => {
        (v1._1 + v2._1, v1._2 + v2._2)
      })

    rdd1.collect().foreach(println)

    val rdd2 = rdd1.map((k) => {
      (k._1, k._2._2 * 1.0 / k._2._1)
    })


    rdd2.collect().foreach(println)

    sc.stop()

  }

}
