package tiger.sparkstreaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @Author Zenghu
 * @Date 2021/5/15 22:48
 * @Description
 * @Version: 1.0
 *
 * */
object SparkStreamingWindow {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WindowTest")
    conf.setMaster("local[*]")

    val ss = new StreamingContext(conf, Seconds(5))


    val ds = ss.socketTextStream("192.168.100.4", 9999)


    // 窗口长度必须是批处理的长度的整倍数
    // 滑动长度也必须是批处理长度的整数倍
    // 情况1： 10秒计算一次，窗口每次滑动5秒，存在重复处理
    val dsWindows: DStream[String] = ds.window(Seconds(10), Seconds(10))


    val wordCount = dsWindows.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)


    wordCount.print()


    ss.start()

    ss.awaitTermination()
  }

}
