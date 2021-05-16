package tiger.sparkstreaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 *
 * @Author Zenghu
 * @Date 2021/5/13 21:20
 * @Description
 * @Version: 1.0
 *
 * */
object SparkStreamingRDDQueue {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")

    val streaming = new StreamingContext(conf, Seconds(5))


    val rddQueue = new mutable.Queue[RDD[String]]()

    val inputStreaming = streaming.queueStream(rddQueue, false)

    val lines = streaming.socketTextStream("192.168.100.4", 9999)

    lines.foreachRDD(rddQueue += _)


    val res = inputStreaming.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    res.print()


    //streaming.stop()

    streaming.start()

    streaming.awaitTermination()
  }

}
