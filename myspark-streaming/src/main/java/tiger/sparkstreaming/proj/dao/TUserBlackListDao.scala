package tiger.sparkstreaming.proj.dao

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 15:55
 * @Description
 * @Version: 1.0
 *
 * */
trait TUserBlackListDao {

  def add(user: String): Boolean

  def exists(user: String): Boolean

  def list(): Set[String]
}
