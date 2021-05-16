package tiger.sparkstreaming.demo

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import redis.clients.jedis.Jedis

/**
 *
 * @Author Zenghu
 * @Date 2021/5/15 23:01
 * @Description
 * @Version: 1.0
 *
 * */
object SparkStreamingGracefullyClose {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WindowTest")
    conf.setMaster("local[*]")

    val ss = new StreamingContext(conf, Seconds(5))

    val topics = Set("test")
    val kafkaParams = Map[String, Object](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.100.4:9092,192.168.100.5:9092,192.168.100.6:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "group1",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

    val kafkaStreaming: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ss, PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))


    val wordCount = kafkaStreaming.flatMap(_.value().split(" ")).map((_, 1)).reduceByKey(_ + _)


    wordCount.print()


    ss.start()


    new Thread(() => {
      val jedis = new Jedis("192.168.100.4", 6379, 3000)
      jedis.auth("tiger")
      while (true) {
        val flag: String = jedis.get("sparkStop")
        if (flag != null && flag.equals("1") && ss.getState() == StreamingContextState.ACTIVE) {
          println("close")
          ss.stop(true, true)
          System.exit(0)
        }
        TimeUnit.SECONDS.sleep(5)
      }
      if (jedis != null)
        jedis.close()
    }).start()

    ss.awaitTermination()
  }

}
