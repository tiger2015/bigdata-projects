package tiger.sparkcore.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Author Zenghu
  * @Date 2021/4/10 22:40
  * @Description
  * @Version: 1.0
  *
  **/
object SparkTransform_sortBy {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")

    val sc = new SparkContext(conf)

    val rdd0 = sc.makeRDD(List(("1", 1), ("2", 2), ("11", 3), ("1", 4), ("0", 5)), 3)
    rdd0.saveAsTextFile("output1")
    val rdd1 = rdd0.sortBy(data => data)

    //rdd1.collect().foreach(println)
    rdd1.saveAsTextFile("output2")

    sc.stop()

  }

}
