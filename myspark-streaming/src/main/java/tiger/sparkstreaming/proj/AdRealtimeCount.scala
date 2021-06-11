package tiger.sparkstreaming.proj

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import tiger.sparkstreaming.proj.common.{AreaCityAdCountHandler, UserBlackListHandler}
import tiger.sparkstreaming.proj.entity.AdClick

import scala.collection.mutable

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 19:28
 * @Description
 * @Version: 1.0
 *
 * */
object AdRealtimeCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("SparkStreaming")
    //conf.setMaster("spark://node01:7077")
    //conf.setMaster("local[*]")

    val streamingContext = new StreamingContext(conf, Seconds(5))

    val kafkaParams = new mutable.HashMap[String, Object]()
    kafkaParams.update(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.4:9092,192.168.100.5:9092,192.168.100.6:9092")
    kafkaParams.update(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    kafkaParams.update(ConsumerConfig.GROUP_ID_CONFIG, "area_city_ad_count")
    kafkaParams.update(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    kafkaParams.update(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    kafkaParams.update(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    val topics = Set("ad_click")

    val kafkaStreaming = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    val clickDS: DStream[AdClick] = kafkaStreaming.map(kv => {
      AdClick.parseFromJson(kv.value())
    })

    // 过滤黑名单中的用户
    val filteredDS = UserBlackListHandler.filterBlackListUser(clickDS)
    // 保存黑名单用户
    UserBlackListHandler.saveUserBlackList(filteredDS)

    filteredDS.cache()

    // 保存每个区域每个城市的每个广告点击数
    AreaCityAdCountHandler.saveAreaCityAdCount(filteredDS)

    // 统计每个小时的点击量

    val result: DStream[(String, List[(String, Long)])] = AreaCityAdCountHandler.getAdHourCount(filteredDS)

    result.print()



    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
