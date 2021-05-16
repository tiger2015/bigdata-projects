package tiger.sparkstreaming.proj.entity

import java.util.{Calendar, Date}

/**
 * 用户点击广告统计
 *
 * @Author Zenghu
 * @Date 2021/5/16 16:05
 * @Description
 * @Version: 1.0
 *
 * */
class UserAdCount(dateIn: Date = Calendar.getInstance().getTime) extends Serializable {
  var clickTime: Date = dateIn
  var userId: String = ""
  var adId: String = ""
  var count: Int = 0


  def canEqual(other: Any): Boolean = other.isInstanceOf[UserAdCount]

  override def equals(other: Any): Boolean = other match {
    case that: UserAdCount =>
      (that canEqual this) &&
        clickTime == that.clickTime &&
        userId == that.userId &&
        adId == that.adId
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(clickTime, userId, adId)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
