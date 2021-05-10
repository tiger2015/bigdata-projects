package tiger.sparksql.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *
 * @Author Zenghu
 * @Date 2021/5/9 17:12
 * @Description
 * @Version: 1.0
 *
 * */
object SparkHive02 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("hive-sql")
    conf.setMaster("local[*]")

    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()


    spark.sql("use test")


    spark.sql(
      """
        |select
        |c.area, c.city_name, c.city_id, p.product_name
        |from user_visit_action a
        |join product_info p on a.click_product_id = p.product_id
        |join city_info c on a.city_id = c.city_id
        |where a.click_product_id >= 0
        |
        |
        |""".stripMargin).createOrReplaceTempView("t1")


    spark.udf.register("productPer", functions.udaf(new MyUdaf()))

    // 统计不同区域不同城市的不同产品的点击数
    // 使用自定义的聚集函数计算百分比
    spark.sql(
      """
        |select
        |  area, product_name, count(*) as cnt, productPer(city_name)
        |from t1 group by area, product_name
        |
        |""".stripMargin).createOrReplaceTempView("t2")


    // 每个地区的商品排名

    spark.sql(
      """
        |select
        | t2.*, RANK() OVER (PARTITION BY area ORDER BY cnt DESC ) as rnk
        |
        |from t2
        |
        |""".stripMargin).createOrReplaceTempView("t3")


    // 取前三名
    spark.sql(
      """
        |
        | select
        |  * from t3 where rnk <= 3
        |
        |""".stripMargin).createOrReplaceTempView("t4")


    spark.sql("select * from t4").show(truncate=false)


    spark.close()


  }


  case class Buffer(var total: Long, var map: mutable.HashMap[String, Long])


  class MyUdaf extends Aggregator[String, Buffer, String] {
    override def zero: Buffer = new Buffer(0, new mutable.HashMap[String, Long]())

    override def reduce(b: Buffer, a: String): Buffer = {
      b.total += 1
      var count = b.map.getOrElse(a, 0L)
      count += 1L
      b.map.update(a, count)
      b
    }

    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total += b2.total
      b2.map.foreach(kv => {
        var count = b1.map.getOrElse(kv._1, 0L)
        count += kv._2
        b1.map.update(kv._1, count)
      })
      b1
    }

    override def finish(reduction: Buffer): String = {

      val list = reduction.map.toList.sortWith((v1, v2) => {
        (v1._2 > v2._2)
      }).take(2)


      val hasMore = reduction.map.size > 2
      var sum = 0L

      val res = ListBuffer[String]()

      list.foreach(kv => {
        val p = kv._2 * 100.0 / reduction.total
        sum += kv._2
        res.append(s"${kv._1} ${p.formatted("%.2f")}")
      })

      if (hasMore) {
        val p = (reduction.total - sum) * 100.0 / reduction.total
        res.append(s"其他${p.formatted("%.2f")}")
      }

      res.mkString(",")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }


}
