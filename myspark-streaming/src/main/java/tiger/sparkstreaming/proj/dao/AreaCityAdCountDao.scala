package tiger.sparkstreaming.proj.dao

import java.text.SimpleDateFormat

import tiger.sparkstreaming.proj.entity.AreaCityAdCount
import tiger.sparkstreaming.proj.util.DBConnectionUtil

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 17:28
 * @Description
 * @Version: 1.0
 *
 * */
class AreaCityAdCountDao extends TAreaCityAdCountDao {
  override def insert(areaCityAdCount: AreaCityAdCount): Boolean = {
    val connection = DBConnectionUtil.getConnection()
    val statement = connection.prepareStatement(
      """
        | insert into `area_city_ad_count` (`dt`, `area`, `city`, `ad`, `count`) values(?, ?, ?, ?, ?)
        | on duplicate key update `count` = `count` + ?
        |
        |""".stripMargin)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    statement.setString(1, format.format(areaCityAdCount.date))
    statement.setString(2, areaCityAdCount.area)
    statement.setString(3, areaCityAdCount.city)
    statement.setString(4, areaCityAdCount.ad)
    statement.setInt(5, areaCityAdCount.count)
    statement.setInt(6, areaCityAdCount.count)

    val flag = statement.execute()

    statement.close()
    connection.close()

    flag
  }

  override def getCount(areaCityAdCount: AreaCityAdCount): Int = {
    val connection = DBConnectionUtil.getConnection()
    val statement = connection.prepareStatement(
      """
        | select `count` from `area_city_ad_count` where
        | `dt` = ? and `area` = ? and `city` = ? and `ad` = ?
        |
        |""".stripMargin)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    statement.setString(1, format.format(areaCityAdCount.date))
    statement.setString(2, areaCityAdCount.area)
    statement.setString(3, areaCityAdCount.city)
    statement.setString(4, areaCityAdCount.ad)

    val resultSet = statement.executeQuery()

    var count = 0
    if (resultSet.next()) {
      count = resultSet.getInt(1)
    }

    resultSet.close()
    statement.close()
    connection.close()

    count
  }
}
