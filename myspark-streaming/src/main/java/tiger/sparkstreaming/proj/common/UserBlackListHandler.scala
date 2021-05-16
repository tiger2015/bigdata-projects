package tiger.sparkstreaming.proj.common

import java.text.SimpleDateFormat

import org.apache.spark.streaming.dstream.DStream
import tiger.sparkstreaming.proj.dao.{UserAdCountDao, UserBlackListDao}
import tiger.sparkstreaming.proj.entity.{AdClick, UserAdCount}

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 18:11
 * @Description
 * @Version: 1.0
 *
 * */
object UserBlackListHandler {


  /**
   * 过滤黑名单中的用户
   *
   * @param adClickDS
   * @return
   */
  def filterBlackListUser(adClickDS: DStream[AdClick]): DStream[AdClick] = {

    // TODO 从数据库中查询黑名单
    val userBlackListDao = new UserBlackListDao()
    val blackList = userBlackListDao.list()
    // 过滤黑名单
    adClickDS.filter(v => {
      !blackList.contains(v.user)
    })
  }


  def saveUserBlackList(adClickDS: DStream[AdClick]) = {

    val transformDS: DStream[((String, String, String), Int)] = adClickDS.transform(rdd => {
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      rdd.map(v => {
        val time = simpleDateFormat.format(v.clickTime)
        val user = v.user
        val ad = v.ad
        ((time, user, ad), 1)
      }).reduceByKey(_ + _)
    })

    transformDS.reduceByKey(_ + _).foreachRDD(rdd => {
      // TODO 对每个分区进行计算
      rdd.foreachPartition(iter => {
        val userAdCountDao = new UserAdCountDao
        val userBlackListDao = new UserBlackListDao
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        iter.foreach({
          case ((clickTime, user, ad), count) => {
            val date = simpleDateFormat.parse(clickTime)
            val total = userAdCountDao.select(date, user, ad).count + count
            // 大于30则加入黑名单
            if (total > 30) {
              userBlackListDao.add(user)
            }
            val userAdCount = new UserAdCount(date)
            userAdCount.adId = ad
            userAdCount.userId = user
            userAdCount.count = count
            userAdCountDao.insert(userAdCount)
          }
        })
      })
    })
  }
}
