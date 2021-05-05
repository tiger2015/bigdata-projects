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
object SparkTransform_distinct {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")

    val sc = new SparkContext(conf)

    val rdd0: RDD[Int] = sc.makeRDD(List(1,2,3,4,4,5,5,6,1), 3)

    //val rdd1: RDD[Int] = rdd0.distinct(2)

    val rdd1: RDD[(Int, Int)] = rdd0.map((_,1))

    val rdd2 = rdd1.reduceByKey((data1, data2) =>data1)

    val rdd3 = rdd2.map(_._1)



    rdd3.collect().foreach(println)




    sc.stop()

  }

}
