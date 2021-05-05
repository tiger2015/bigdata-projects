package tiger.proj01.service

/**
 *
 * @Author Zenghu
 * @Date 2021/4/24 23:03
 * @Description
 * @Version: 1.0
 *
 * */
trait TUserVisitActionService {

  def topNCategory(path: String, n: Int = 10): Any

  def topNSessionByCategory(path: String, categories:List[Long], n:Int = 10): Any

  def pageTransferRate(path: String): Any
}
