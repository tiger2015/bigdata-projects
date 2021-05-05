package tiger.sparkcore.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  *
  * @Author Zenghu
  * @Date 2021/4/10 22:40
  * @Description
  * @Version: 1.0
  *
  **/
object SparkTransform_partitionBy {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")

    val sc = new SparkContext(conf)

    val rdd0 = sc.makeRDD(List(("a", 1), ("b", 2), (2, 3), ("c", "4")), 2)


    val rdd1 = rdd0.partitionBy(new HashPartitioner(3))


    rdd1.saveAsTextFile("output")







    sc.stop()

  }

}
