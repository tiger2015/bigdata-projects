package tiger.sparkcore.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Author Zenghu
  * @Date 2021/4/14 22:30
  * @Description
  * @Version: 1.0
  *
  **/
object SparkSerial_Test2 {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("serial")
    // 注册使用kryo序列化方式
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Search]))

    val sc = new SparkContext(conf)


    val rdd0 = sc.makeRDD(List("hello", "spark", "hadoop", "hive"))
    
    val search = new Search("h")

    val result = search.getMatch1(rdd0)

    result.collect().foreach(println)

    sc.stop()

  }

}

// query为成员变量
class Search(query: String) extends Serializable {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatch1(rdd: RDD[String]) = {
    rdd.filter(isMatch)
  }

  def getMatch2(rdd: RDD[String]) = {
    val  s= query  // 将成员变量转换成局部变量，则Search类不需要序列化，此过程在Driver端执行
    rdd.filter(_.contains(s)) // 在Executor端执行
  }
}