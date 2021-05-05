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
object SparkTransform_aggregateByKey  {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")

    val sc = new SparkContext(conf)

    val rdd0 = sc.makeRDD(List(("a", 1), ("b", 3), ("c", 4), ("a", 2), ("c", 6)), 2)

    def seqOp(x: Int, y: Int): Int = {
      math.max(x, y)
    }

    def comOp(x: Int, y: Int): Int = {
      x + y
    }


    val rdd1 = rdd0.aggregateByKey(0)(seqOp, comOp)

    rdd1.collect().foreach(println)

    sc.stop()

  }

}
