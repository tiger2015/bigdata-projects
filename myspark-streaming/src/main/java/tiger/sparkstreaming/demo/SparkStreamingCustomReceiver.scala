package tiger.sparkstreaming.demo

import java.io.{BufferedReader, InputStreamReader}
import java.net.{InetSocketAddress, Socket}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @Author Zenghu
 * @Date 2021/5/13 22:50
 * @Description
 * @Version: 1.0
 *
 * */
object SparkStreamingCustomReceiver {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")
    val streaming = new StreamingContext(conf, Seconds(5))
    val receive = streaming.receiverStream(new CustomReceiver("192.168.100.4", 9999))
    val res = receive.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    res.print()

    streaming.start()
    streaming.awaitTermination()
  }


}

class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {



  override def onStart(): Unit = {
    new Thread(() => {
      receive()
    }).start()

  }


  def receive() = {
    val socket = new Socket() // 不能被序列化,只能作为局部变量使用
    socket.connect(new InetSocketAddress(host, port))
    while(!socket.isConnected)
    println("connected")
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    var line: String = null
    while ((line = reader.readLine()) != null) {
      store(line)
    }
  }

  override def onStop(): Unit = {
  }
}
