package tiger.sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Author Zenghu
  * @Date 2021/4/13 23:03
  * @Description
  * @Version: 1.0
  *
  **/
object SparkAction_aggregate {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("action")

    val sc = new SparkContext(conf)

    val rdd0: RDD[Int] = sc.makeRDD(List(1, 2, -3, 4))

    val sum = rdd0.aggregate(10)(_ + _, _ + _)

    // 求最小值
    val minVal = rdd0.aggregate(Int.MaxValue)((u, v) => {
      math.min(u, v)
    }, (u1, u2) => {
      math.min(u1, u2)
    })


    println(minVal)


    val minVal1 = rdd0.fold(Int.MaxValue)((u, v) => math.min(u, v))

    println(minVal1)

    sc.stop()

  }


}
