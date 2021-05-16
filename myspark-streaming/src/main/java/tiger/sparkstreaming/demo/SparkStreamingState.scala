package tiger.sparkstreaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @Author Zenghu
 * @Date 2021/5/15 18:31
 * @Description
 * @Version: 1.0
 *
 * */
object SparkStreamingState {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("State")

    val sc = new StreamingContext(conf, Seconds(5))

    sc.checkpoint("checkpoint")

    val ds: ReceiverInputDStream[String] = sc.socketTextStream("192.168.100.4", 9999)

    val dsMap: DStream[(String, Int)] = ds.flatMap(line => {
      line.split(" ").map((_, 1))
    })

    val res = dsMap.reduceByKey(_ + _)
    val resRD = res.updateStateByKey[Int](updateFunc _)

    resRD.print()

    sc.start()
    sc.awaitTermination()
  }


  def updateFunc(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val total = newValues.foldLeft(0)(_ + _) + runningCount.getOrElse(0)
    Some(total)
  }
}
