package tiger.sparkcore.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Author Zenghu
  * @Date 2021/4/18 11:24
  * @Description
  * @Version: 1.0
  *
  **/
object SparkAcc_Test01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Acc")

    val sc = new SparkContext(conf)


    val rdd0 = sc.makeRDD(List(1, 2, 3, 4, 5))

    // 定义累加器
    val sum = sc.longAccumulator("sum")

    //var sum = 0 // Driver端

    rdd0.foreach(data => { // Executor端
      // 使用累加器
      sum.add(data)
    })


    println(sum.value)


    sc.stop()

  }


}
