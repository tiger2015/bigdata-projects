package tiger.sparkstreaming.proj.dao

import java.sql.ResultSet

import tiger.sparkstreaming.proj.util.DBConnectionUtil

import scala.collection.mutable

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 15:56
 * @Description
 * @Version: 1.0
 *
 * */
class UserBlackListDao extends TUserBlackListDao {

  override def add(user: String): Boolean = {
    val connection = DBConnectionUtil.getConnection()
    val statement = connection.prepareStatement(
      """
        |insert into `black_list`(`user`) values(?)
        |ON DUPLICATE KEY update `user` = ?
        |
        |
        |""".stripMargin)
    statement.setString(1, user)
    statement.setString(2, user)

    val bool = statement.execute()
    statement.close()
    connection.close()
    bool
  }

  override def exists(user: String): Boolean = {
    val connection = DBConnectionUtil.getConnection()
    val statement = connection.prepareStatement(
      """
        |select `user` from `black_list`
        |where `user` = ?
        |
        |""".stripMargin)
    statement.setString(1, user)

    val result: ResultSet = statement.executeQuery()
    val flag = result.next()
    result.close()
    statement.close()
    connection.close()
    flag

  }

  override def list(): Set[String] = {

    val connection = DBConnectionUtil.getConnection()
    val statement = connection.prepareStatement(
      """
        |select `user` from `black_list`

        |
        |""".stripMargin)

    val resultSet = statement.executeQuery()
    val set = new mutable.HashSet[String]()

    while (resultSet.next()) {
      val user = resultSet.getString(1)
      set.add(user)
    }

    resultSet.close()
    statement.close()
    connection.close()

    set.toSet
  }
}
