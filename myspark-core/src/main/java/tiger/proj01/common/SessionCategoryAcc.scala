package tiger.proj01.common

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 *
 * @Author Zenghu
 * @Date 2021/4/27 23:00
 * @Description
 * @Version: 1.0
 *
 * */
// 输入：(商品类别，session, 点击数)
// 返回：(session, 点击数)
class SessionCategoryAcc extends AccumulatorV2[(Long, String, Long), mutable.Map[Long, mutable.Map[String, Long]]] {
  private val sessionCountMap: mutable.Map[Long, mutable.Map[String, Long]] = new mutable.HashMap[Long, mutable.Map[String, Long]]()


  override def isZero: Boolean = sessionCountMap.isEmpty

  override def copy(): AccumulatorV2[(Long, String, Long), mutable.Map[Long, mutable.Map[String, Long]]] = {
    val copyAcc = new SessionCategoryAcc()
    sessionCountMap.foreach(v => copyAcc.sessionCountMap.update(v._1, v._2))
    copyAcc
  }

  override def reset(): Unit = {
    sessionCountMap.clear()
  }

  override def add(v: (Long, String, Long)): Unit = {
    val categorySession: mutable.Map[String, Long] = sessionCountMap.getOrElseUpdate(v._1, new mutable.HashMap[String, Long]())
    val count: Long = categorySession.getOrElseUpdate(v._2, 0L)
    categorySession.update(v._2, count + 1L)
  }

  override def merge(other: AccumulatorV2[(Long, String, Long), mutable.Map[Long, mutable.Map[String, Long]]]): Unit = {
    other.value.foreach(item => {
      val sessionCategoryMap: mutable.Map[String, Long] = sessionCountMap.getOrElseUpdate(item._1, new mutable.HashMap[String, Long]())
      item._2.foreach(subItem=>{
        val count: Long = sessionCategoryMap.getOrElseUpdate(subItem._1, 0L)
        sessionCategoryMap.update(subItem._1, count + subItem._2)
      })
    })
  }

  override def value: mutable.Map[Long, mutable.Map[String, Long]] = {
    sessionCountMap
  }
}
