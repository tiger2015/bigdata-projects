package tiger.sparkcore.dependency

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Author Zenghu
  * @Date 2021/4/17 19:57
  * @Description
  * @Version: 1.0
  *
  **/
object WordCount {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("dep")

    val sc = new SparkContext(conf)

    val rdd0 = sc.makeRDD(List("Hello World", "Hello Spark"))

    //println(rdd0.toDebugString)
    println(rdd0.dependencies)

    val rdd1 = rdd0.flatMap(line => {
      line.split(" ").map(_.toLowerCase)
    })

    println("============")
    //println(rdd1.toDebugString)
    println(rdd1.dependencies)

    val rdd2 = rdd1.map((_, 1))

    println("==========")
    //println(rdd2.toDebugString)
    println(rdd2.dependencies)

    val rdd3 = rdd2.reduceByKey(_ + _)
    println("============")
    //println(rdd3.toDebugString)
    println(rdd3.dependencies)

    rdd3.collect().foreach(println)

    sc.stop()

  }


}
