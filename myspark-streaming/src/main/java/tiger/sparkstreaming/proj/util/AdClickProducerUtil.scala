package tiger.sparkstreaming.proj.util

import java.util
import java.util.Calendar
import java.util.concurrent.{ExecutorService, Executors, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import tiger.sparkstreaming.proj.entity.AdClick

import scala.collection.mutable
import scala.util.Random

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 10:58
 * @Description
 * @Version: 1.0
 *
 * */
object AdClickProducerUtil {


  def main(args: Array[String]): Unit = {
    val kafkaConfig = new util.HashMap[String, Object]()

    kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.4:9092,192.168.100.5:9092,192.168.100.6:9092")
    kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val topic = "ad_click"


    val adList = List("A", "B", "C", "D", "E", "F", "G")

    val areaList = List("东北", "华北", "华东", "华南", "华中", "西南", "西北")

    val cityMap = Map({
      "东北" -> List("沈阳", "哈尔滨", "大连")
    },
      {
        "华北" -> List("北京", "天津", "石家庄", "济南")
      },
      {
        "华东" -> List("上海", "南京", "杭州")
      },
      {
        "华南" -> List("广州", "深圳", "南昌")
      },
      {
        "华中" -> List("武汉", "长沙")
      },
      {
        "西南" -> List("成都", "重庆")
      },
      {
        "西北" -> List("西安", "兰州")
      },
    )


    val kafkaProducer = new KafkaProducer[String, String](kafkaConfig)

    val threadPool = new ScheduledThreadPoolExecutor(4)

    for (i <- 1 to 4) {
      threadPool.scheduleAtFixedRate(new ProduceAdClickTask, i*1000, 1000, TimeUnit.MILLISECONDS)
    }


    class ProduceAdClickTask extends Runnable {
      override def run(): Unit = {
        val random = new Random()
        val ad = adList(random.nextInt(adList.size))
        val area = areaList(random.nextInt(areaList.size))
        val cityList = cityMap(area)
        val city = cityList(random.nextInt(cityList.size))
        val user = random.nextInt(30).toString

        val adClick = new AdClick(Calendar.getInstance().getTime, area, city, user, ad)

        val record = new ProducerRecord[String, String](topic, "ad", AdClick.toJsonString(adClick))

        kafkaProducer.send(record)
      // println("send")
      }
    }


  }


}
