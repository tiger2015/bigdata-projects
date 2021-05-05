package tiger.proj01.dao

import org.apache.spark.rdd.RDD
import tiger.proj01.common.ApplicationContext

/**
 *
 * @Author Zenghu
 * @Date 2021/4/24 23:02
 * @Description
 * @Version: 1.0
 *
 * */
trait TUserVisitActionDao {

  def readTextFile(path: String): RDD[String] = {
    ApplicationContext.sc.textFile(path)
  }

  def saveResult[T](rdd: RDD[T], path: String)
}
