package tiger.sparksql.basic

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

/**
 *
 * @Author Zenghu
 * @Date 2021/5/5 16:34
 * @Description
 * @Version: 1.0
 *
 * */
object SparkSQL_UDAF02 {

  def main(args: Array[String]): Unit = {


    val config = new SparkConf()
    config.setMaster("local[*]")
    config.setAppName("SparkSQL")
    // TODO 创建SparkSession
    val spark = SparkSession.builder().config(config).getOrCreate()

    // TODO 读取json格式的数据
    val df: DataFrame = spark.read.json("data/user.json")
    import spark.implicits._
    val ds = df.as[User]

    val myUDAF02 = new MyUDAF02


    // TODO 将自定义函数转换成查询列
    val column = myUDAF02.toColumn
    //val res = ds.select(column)

    // TODO 注册函数
    spark.udf.register("avg_age", functions.udaf(new MyUDAF03))
    ds.createOrReplaceTempView("user")
    val res = spark.sql("select avg_age(age) from user")


    res.show()





    // TODO 关闭SparkSession
    spark.close()


  }


  case class CountInfo(var total: Long, var count: Int)

  case class User(name: String, age: Long)


  class MyUDAF02 extends Aggregator[User, CountInfo, Double] {
    // 零值，初始化
    override def zero: CountInfo = {
      CountInfo(0L, 0)
    }

    override def reduce(b: CountInfo, a: User): CountInfo = {
      b.count = b.count + 1
      b.total = b.total + a.age
      b
    }

    override def merge(b1: CountInfo, b2: CountInfo): CountInfo = {
      b1.count = b1.count + b2.count
      b1.total = b1.total + b2.total
      b1

    }

    override def finish(reduction: CountInfo): Double = {
      reduction.total * 1.0 / reduction.count
    }

    override def bufferEncoder: Encoder[CountInfo] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }


  class MyUDAF03 extends Aggregator[Long, CountInfo, Double] {
    // 零值，初始化
    override def zero: CountInfo = {
      CountInfo(0L, 0)
    }

    override def reduce(b: CountInfo, a: Long): CountInfo = {
      b.count += 1
      b.total += a
      b

    }

    override def merge(b1: CountInfo, b2: CountInfo): CountInfo = {
      b1.count += b2.count
      b1.total += b2.total
      b1
    }

    override def finish(reduction: CountInfo): Double = {
      reduction.total * 1.0 / reduction.count
    }

    override def bufferEncoder: Encoder[CountInfo] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

}
