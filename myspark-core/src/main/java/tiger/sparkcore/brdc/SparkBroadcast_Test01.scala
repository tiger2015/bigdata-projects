package tiger.sparkcore.brdc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Author Zenghu
  * @Date 2021/4/18 11:55
  * @Description
  * @Version: 1.0
  *
  **/
object SparkBroadcast_Test01 {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("brdc")

    val sc = new SparkContext(conf)


    val rdd0 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)))

    val map: Map[String, Int] = Map(("a", 1), ("b", 2), ("c", 3))

    val brdcMap: Broadcast[Map[String, Int]] = sc.broadcast(map)


    val rdd1 = rdd0.map({
      case (k1, v1) => {
        val v2 = brdcMap.value.getOrElse(k1, 0)
        if (v2 == 0) {
          (k1, v1)
        } else
          (k1, (v2, v1))
      }
    })


    rdd1.collect().foreach(println)


    sc.stop()

  }


}
