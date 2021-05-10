package tiger.sparksql.basic

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 *
 * @Author Zenghu
 * @Date 2021/5/5 16:34
 * @Description
 * @Version: 1.0
 *
 * */
object SparkSQL_UDAF01 {

  def main(args: Array[String]): Unit = {


    val config = new SparkConf()
    config.setMaster("local[*]")
    config.setAppName("SparkSQL")
    // TODO 创建SparkSession
    val spark = SparkSession.builder().config(config).getOrCreate()

    // TODO 读取json格式的数据
    val df: DataFrame = spark.read.json("data/user.json")
    //df.show()

    // TODO 创建视图
    df.createOrReplaceTempView("user")

    spark.udf.register("avg_age", new UserUDF())

    val ret = spark.sql("select avg_age(age) from user")

    ret.show()


    // TODO 关闭SparkSession
    spark.close()


  }


  class UserUDF extends UserDefinedAggregateFunction {
    // 输入数据类型
    override def inputSchema: StructType = {
      StructType(Array(
        StructField("age", IntegerType)
      ))
    }

    // 缓存数据类型
    override def bufferSchema: StructType = {

      StructType(Array(
        StructField("total", LongType),
        StructField("count", IntegerType)
      ))


    }

    // 输出数据类型
    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    // buffer初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0
    }

    // buffer更新，即累加
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getInt(0)
      buffer(1) = buffer.getInt(1) + 1
    }

    // buffer合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
    }

    // 计算结果
    override def evaluate(buffer: Row): Double = {
      buffer.getLong(0) * 1.0 / buffer.getInt(1)
    }
  }


}
