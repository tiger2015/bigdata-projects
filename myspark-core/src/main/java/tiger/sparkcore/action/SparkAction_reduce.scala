package tiger.sparkcore.action

import com.google.common.collect.MultimapBuilder.SortedSetMultimapBuilder
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
object SparkAction_reduce {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("action")

    val sc = new SparkContext(conf)

    val rdd0: RDD[Int] = sc.makeRDD(List(5, 2, 3, 1, 4))


    val sum = rdd0.reduce(_ + _)

    println(sum)


    val count = rdd0.count()
    println(count)


    val first = rdd0.first()
    println(first)



    val ordered = rdd0.takeOrdered(3)(Ordering.Int.reverse)

    println(ordered.mkString(","))

    sc.stop()

  }


}
