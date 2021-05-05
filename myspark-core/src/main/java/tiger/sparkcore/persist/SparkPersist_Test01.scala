package tiger.sparkcore.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Author Zenghu
  * @Date 2021/4/17 21:58
  * @Description
  * @Version: 1.0
  *
  **/
object SparkPersist_Test01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("dep")

    val sc = new SparkContext(conf)

    val rdd0 = sc.makeRDD(List("Hello World", "Hello Spark"))

    val rdd1 = rdd0.flatMap(_.split(" ").map(_.toLowerCase))

    val rdd2 = rdd1.map(word => {
      println("=================")
      (word, 1)
    })

    rdd2.cache()


    val rdd3 = rdd2.reduceByKey(_ + _)
    rdd3.collect().foreach(println)

    val rdd4 = rdd2.groupByKey()

    rdd4.collect()


    sc.stop()

  }

}
