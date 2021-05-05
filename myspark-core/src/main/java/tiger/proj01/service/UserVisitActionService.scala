package tiger.proj01.service

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import tiger.proj01.common.{ActionType, ApplicationContext, SessionCategoryAcc}
import tiger.proj01.bean.UserVisitAction
import tiger.proj01.dao.UserVisitActionDao

import scala.collection.mutable

/**
 *
 * @Author Zenghu
 * @Date 2021/4/24 23:06
 * @Description
 * @Version: 1.0
 *
 * */
class UserVisitActionService extends TUserVisitActionService {
  private val clickActionDao = new UserVisitActionDao

  override def topNCategory(path: String, n: Int): Array[(Long, (Long, Long, Long))] = {
    val userActionRDD: RDD[UserVisitAction] = clickActionDao.readClickActionLog(path)
    // 将对象转换成=> ((商品类别, (点击数、订单数、支付数))
    val categoryRDD: RDD[(Long, (Long, Long, Long))] = userActionRDD.flatMap(action => {
      action.actionType match {
        case actionType if actionType == ActionType.CLICK => List((action.clickCategoryId, (1L, 0L, 0L)))
        case actionType if actionType == ActionType.ORDER => action.orderCategoryIds.map(item => (item, (0L, 1L, 0L)))
        case actionType if actionType == ActionType.PAY => action.payCategoryIds.map(item => (item, (0L, 0L, 1L)))
        case _ => Nil
      }
    })
    val categoryClickRDD: RDD[(Long, (Long, Long, Long))] = categoryRDD.reduceByKey((v1, v2) => {
      (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3)
    })
    categoryClickRDD.sortBy(_._2, ascending = false).take(n)
  }

  /**
   * 统计跳转率
   *
   * @param path
   * @return
   */
  override def pageTransferRate(path: String): RDD[((Long, Long), Double)] = {
    val userActionRDD: RDD[UserVisitAction] = clickActionDao.readClickActionLog(path)
    userActionRDD.persist(StorageLevel.MEMORY_AND_DISK)
    // 每个页面的访问数
    val pageVisitRDD: RDD[(Long, Long)] = userActionRDD.map(action => {
      (action.pageId, 1L)
    })
    val clickCount: Array[(Long, Long)] = pageVisitRDD.reduceByKey(_ + _).collect()

    val clickCountMap: Map[Long, Long] = clickCount.toMap
    // 广播变量
    // 每个页面被点击的次数
    val brdc = ApplicationContext.sc.broadcast(clickCountMap)

    val sessionVisitPageRDD: RDD[(String, (String, Long))] = userActionRDD.map(action => {
      (action.sessionId, (action.actionTime, action.pageId))
    })

    // shuffle
    // ((页面1,页面2), 跳转次数)
    val pageJumpRDD: RDD[((Long, Long), Long)] = sessionVisitPageRDD.groupByKey().flatMap(pages => {
      val ordered = mutable.TreeMap[String, Long]()
      pages._2.foreach(page => ordered.update(page._1, page._2))
      var firstPage: Long = ordered.getOrElse(ordered.firstKey, -1L)
      ordered.remove(ordered.firstKey)
      var list = List[((Long, Long), Long)]()
      for (v: Long <- ordered.values) {
        list = list :+ ((firstPage, v), 1L)
        firstPage = v
      }
      list
    }).reduceByKey(_ + _)

    // 计算跳转率
    val jumpRateRDD: RDD[((Long, Long), Double)] = pageJumpRDD.map(v => {
      val a: Long = brdc.value.getOrElse(v._1._1, 0)
      if (a == 0) {
        (v._1, 0.0D)
      } else (v._1, v._2 * 1.0 / a)
    })
    jumpRateRDD.sortBy(_._2, false)
  }

  /**
   * 获取指定类别的前n个session
   *
   * @param path
   * @param categories
   * @param n
   * @return
   */
  override def topNSessionByCategory(path: String, categories: List[Long], n: Int = 10): Array[(Long, List[(String, Long)])] = {
    val userVisitActionRDD: RDD[UserVisitAction] = clickActionDao.readClickActionLog(path)
    userVisitActionRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val brdc = ApplicationContext.sc.broadcast(categories)


    // 方法1
    /**
     * val sessionCategoryRDD: RDD[((Long, String), Long)] = userVisitActionRDD.flatMap(f = action => {
     * action.actionType match {
     * case actionType if actionType == ActionType.CLICK =>
     * if (brdc.value.contains(action.clickCategoryId)) {
     * List(((action.clickCategoryId, action.sessionId), 1L))
     * } else {
     * Nil
     * }
     * case actionType if actionType == ActionType.ORDER =>
     * action.orderCategoryIds.filter(brdc.value.contains).map(id => {
     * ((id, action.sessionId), 1L)
     * })
     * case actionType if actionType == ActionType.PAY =>
     * action.payCategoryIds.filter(brdc.value.contains).map(id => {
     * ((id, action.sessionId), 1L)
     * })
     * case _ => Nil
     * }
     * }).reduceByKey(_ + _)
     *
     * //(categoryId, (sessionId, 点击数))
     * val sessionCategoryGroupRDD: RDD[(Long, Iterable[(String, Long)])] = sessionCategoryRDD.map(data => (data._1._1, (data._1._2, data._2))).groupByKey()
     *
     * val result: RDD[(Long, List[(String, Long)])] = sessionCategoryGroupRDD.map(data => {
     * (data._1, data._2.toList.sortBy(_._2)(Ordering.Long.reverse).take(n))
     * })
     * result.collect()
     * */

    // 方法2
    // 定义累加器
    val categorySessionCounter = new SessionCategoryAcc()
    // 注册累加器
    ApplicationContext.sc.register(categorySessionCounter, "sessionCounter")


    val sessionCategoryRDD: RDD[(Long, String, Long)] = userVisitActionRDD.flatMap(f = action => {
      action.actionType match {
        case actionType if actionType == ActionType.CLICK =>
          if (brdc.value.contains(action.clickCategoryId)) {
            List((action.clickCategoryId, action.sessionId, 1L))
          } else {
            Nil
          }
        case actionType if actionType == ActionType.ORDER =>
          action.orderCategoryIds.filter(brdc.value.contains).map(id => {
            (id, action.sessionId, 1L)
          })
        case actionType if actionType == ActionType.PAY =>
          action.payCategoryIds.filter(brdc.value.contains).map(id => {
            (id, action.sessionId, 1L)
          })
        case _ => Nil
      }
    })

    sessionCategoryRDD.foreach(categorySessionCounter.add)

    val result = categorySessionCounter.value

    result.map(item => {
      val sortedSession: List[(String, Long)] = item._2.toList.sortBy(_._2)(Ordering.Long.reverse).take(n)
      (item._1, sortedSession)
    }).toArray
  }
}
