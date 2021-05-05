package tiger.proj01.dao

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import tiger.proj01.bean.UserVisitAction
import tiger.proj01.util.UserVisitActionUtil

/**
 *
 * @Author Zenghu
 * @Date 2021/4/24 23:03
 * @Description
 * @Version: 1.0
 *
 * */
class UserVisitActionDao extends TUserVisitActionDao {

  def readClickActionLog(path: String): RDD[UserVisitAction] = {
    val rdd0: RDD[String] = readTextFile(path)
    rdd0.map(UserVisitActionUtil.parseClickLog)
  }

  override def saveResult[T](rdd: RDD[T], path: String) {
    rdd.saveAsTextFile(path)
  }
}
