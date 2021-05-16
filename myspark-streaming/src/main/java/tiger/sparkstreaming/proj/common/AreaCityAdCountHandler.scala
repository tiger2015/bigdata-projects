package tiger.sparkstreaming.proj.common

import java.text.SimpleDateFormat

import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import tiger.sparkstreaming.proj.dao.AreaCityAdCountDao
import tiger.sparkstreaming.proj.entity.{AdClick, AreaCityAdCount}

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 18:32
 * @Description
 * @Version: 1.0
 *
 * */
object AreaCityAdCountHandler {

  def saveAreaCityAdCount(clickDS: DStream[AdClick]): Unit = {

    val mapDS: DStream[((String, String, String, String), Int)] = clickDS.transform(rdd => {
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      rdd.map(v => {
        ((simpleDateFormat.format(v.clickTime), v.area, v.city, v.ad), 1)
      }).reduceByKey(_ + _)
    })

    mapDS.reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val areaCityAdCountDao = new AreaCityAdCountDao()
        iter.foreach({
          case ((time, area, city, ad), count) => {

            val areaCityAdCount = new AreaCityAdCount()
            areaCityAdCount.date = simpleDateFormat.parse(time)
            areaCityAdCount.area = area
            areaCityAdCount.city = city
            areaCityAdCount.ad = ad
            areaCityAdCount.count = count
            areaCityAdCountDao.insert(areaCityAdCount)
          }
        })
      })
    })
  }

  def getAdHourCount(clickDS: DStream[AdClick]): DStream[(String, List[(String, Long)])] = {


    val mapDS: DStream[((String, String), Long)] = clickDS.map(v => {
      val format = new SimpleDateFormat("hh:mm")
      val time = format.format(v.clickTime)
      ((v.ad, time), 1L)
    }).reduceByKeyAndWindow((x: Long, y: Long) => x + y, Seconds(60), Seconds(10))

    val groupDS: DStream[(String, Iterable[(String, Long)])] = mapDS.map(v => (v._1._1, (v._1._2, v._2))).groupByKey()

    groupDS.map(v => {
      (v._1, v._2.toList.sortBy(_._1))
    })

  }


}
