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
object SparkTransform_cogroup {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")

    val sc = new SparkContext(conf)

    val rdd0 = sc.makeRDD(List(("a",1), ("b",2), ("c", 3), ("a", 7)))
    val rdd1 = sc.makeRDD(List(("a",4), ("b",5), ("d", 5), ("c",8)))


    val cgRDD = rdd0.cogroup(rdd1)

    cgRDD.collect().foreach(println(_))

    sc.stop()

  }

}
