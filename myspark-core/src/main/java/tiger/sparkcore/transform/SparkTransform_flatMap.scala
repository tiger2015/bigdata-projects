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
object SparkTransform_flatMap {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")

    val sc = new SparkContext(conf)

    val rdd0= sc.makeRDD(List(List(1,2), List(2,3,4),5,6), 2)

    val rdd1 = rdd0.flatMap {
      case list: List[Any] => list
      case a => List(a)
    }

    rdd1.collect().foreach(println)

    sc.stop()

  }

}
