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
object SparkTransform_join {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")

    val sc = new SparkContext(conf)

    val rdd0 = sc.makeRDD(List(("a",1), ("b",2), ("c", 3)))
    val rdd1 = sc.makeRDD(List(("a",4), ("b",5), ("d", 5)))

    val joinRDD = rdd0.join(rdd1)

    joinRDD.collect().foreach(println)


    sc.stop()

  }

}
