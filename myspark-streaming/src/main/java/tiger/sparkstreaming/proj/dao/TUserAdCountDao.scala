package tiger.sparkstreaming.proj.dao

import java.util.Date

import tiger.sparkstreaming.proj.entity.UserAdCount

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 16:10
 * @Description
 * @Version: 1.0
 *
 * */
trait TUserAdCountDao {

  def insert(userAdCount: UserAdCount): Boolean


  def select(clickTime: Date, userId: String, adId: String): UserAdCount

}
