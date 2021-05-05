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
object SparkTransform_map {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")

    val sc = new SparkContext(conf)

    val rdd0: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    def myMap(data:Int):Int={
      data * 2
    }

    //val rdd1 = rdd0.map(myMap)

    val rdd1: RDD[Int] = rdd0.map(_*2)

    rdd1.collect().foreach(println)


    sc.stop()

  }

}
