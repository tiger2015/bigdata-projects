package tiger.sparkstreaming.proj.dao

import java.text.SimpleDateFormat
import java.util.Date

import tiger.sparkstreaming.proj.entity.UserAdCount
import tiger.sparkstreaming.proj.util.DBConnectionUtil

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 16:12
 * @Description
 * @Version: 1.0
 *
 * */
class UserAdCountDao extends TUserAdCountDao {


  override def insert(userAdCount: UserAdCount): Boolean = {
    val connection = DBConnectionUtil.getConnection()
    val statement = connection.prepareStatement(
      """
        | insert into `user_ad_count` (`click_date`, `user`, `ad`, `count`) values(?, ?, ?, ?)
        | ON DUPLICATE KEY update `count` = `count` + ?
        |
        |""".stripMargin)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    statement.setString(1, format.format(userAdCount.clickTime))
    statement.setString(2, userAdCount.userId)
    statement.setString(3, userAdCount.adId)
    statement.setInt(4, userAdCount.count)

    statement.setInt(5, userAdCount.count)


    val bool = statement.execute()
    statement.close()
    connection.close()

    bool
  }

  override def select(clickTime: Date, userId: String, adId: String): UserAdCount = {
    val connection = DBConnectionUtil.getConnection()
    val statement = connection.prepareStatement(
      """
        | select `click_date`,`user`,`ad`,`count` from `user_ad_count`
        | where `click_date` = ?  and `user` = ? and `ad` = ?
        |
        |""".stripMargin)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    statement.setString(1, format.format(clickTime))
    statement.setString(2, userId)
    statement.setString(3, adId)
    val resultSet = statement.executeQuery()

    val flag = resultSet.next()

    var result: UserAdCount = new UserAdCount()
    if (flag) {
      val userAdClickCount = new UserAdCount(format.parse(resultSet.getString(1)))
      userAdClickCount.userId = resultSet.getString(2)
      userAdClickCount.adId = resultSet.getString(3)
      userAdClickCount.count = resultSet.getInt(4)
      result = userAdClickCount
    }
    resultSet.close()
    statement.close()
    connection.close()
    result
  }
}
