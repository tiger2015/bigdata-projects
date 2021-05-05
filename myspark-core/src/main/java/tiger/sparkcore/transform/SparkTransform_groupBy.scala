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
object SparkTransform_groupBy {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")

    val sc = new SparkContext(conf)

    val rdd0: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 2)

    val rdd1: RDD[(Int, Iterable[Int])] = rdd0.groupBy(data => {
      1
    })

    rdd0.saveAsTextFile("output1")
    rdd1.saveAsTextFile("output2")

    sc.stop()

  }

}
