package tiger.sparkcore.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  *
  * @Author Zenghu
  * @Date 2021/4/18 11:24
  * @Description
  * @Version: 1.0
  *
  **/
object SparkAcc_Test02 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Acc")

    val sc = new SparkContext(conf)


    val wordCountAcc = new MyWordCountAcc()

    sc.register(wordCountAcc, "wc")

    val rdd0 = sc.makeRDD(List("Hello World", "Hello Spark"))


    val rdd1 = rdd0.flatMap(_.split(" ").map(word => word.toLowerCase))


    rdd1.foreach(word => {
      wordCountAcc.add(word)
    })

    println(wordCountAcc.value)


    sc.stop()

  }

  /**
    * IN: 输入的数据类型
    * OUT: 返回的结果
    *
    *
    */

  class MyWordCountAcc extends AccumulatorV2[String, mutable.Map[String, Int]] {

    private val map = mutable.Map[String, Int]()

    // 变量是否为空

    override def isZero: Boolean = {
      map.isEmpty
    }

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      val copy = new MyWordCountAcc()
      map.foreach(v => copy.map.update(v._1, v._2))
      copy
    }


    // 累加器重置
    override def reset(): Unit = {
      map.clear()
    }

    // 添加值
    override def add(v: String): Unit = {
      val count = map.getOrElse(v, 0)
      map.update(v, count + 1)
    }

    // 累加器的值合并
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      other.value.foreach((v: (String, Int)) => {
        val count = map.getOrElse(v._1, 0) + v._2
        map.update(v._1, count)
      })
    }

    override def value: mutable.Map[String, Int] = {
      map
    }
  }

}
