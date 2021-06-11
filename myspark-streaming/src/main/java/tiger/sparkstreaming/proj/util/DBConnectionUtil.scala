package tiger.sparkstreaming.proj.util

import java.sql.Connection

import com.alibaba.druid.pool.DruidDataSource
import com.alibaba.druid.util.JdbcConstants

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 15:16
 * @Description
 * @Version: 1.0
 *
 * */
object DBConnectionUtil {

  private val driver = "com.mysql.jdbc.Driver"
  private val url = "jdbc:mysql://192.168.100.1:3306/test?useUnicode=true&amp;characterEncoding=utf-8"
  private val password = "test"
  private val username = "test"

  private val druidDataSource = new DruidDataSource()

  druidDataSource.setUrl(url)
  druidDataSource.setUsername(username)
  druidDataSource.setPassword(password)
  druidDataSource.setDbType(JdbcConstants.MYSQL)

  druidDataSource.setInitialSize(2)

  druidDataSource.setMaxActive(10)

  druidDataSource.setMinIdle(2)

  druidDataSource.setMaxWait(10000)

  druidDataSource.setPoolPreparedStatements(true)
  druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(5)

  druidDataSource.init()


  def getConnection(): Connection = {
    druidDataSource.getConnection
  }

  def closeConnection(connection:Connection): Unit ={
    connection.close()
  }

}
