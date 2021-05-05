package tiger.proj01.controller

import tiger.proj01.service.UserVisitActionService

/**
 *
 * @Author Zenghu
 * @Date 2021/4/26 21:37
 * @Description
 * @Version: 1.0
 *
 * */
class UserVisitActionAnalysisController {

  private val clickActionService = new UserVisitActionService()

  def topNCategory(path: String, n: Int) {
    val topNCategories = clickActionService.topNCategory(path, n)
    topNCategories.foreach(println)
  }


  def pageJumpRate(input: String, output: String) {
    val rate = clickActionService.pageTransferRate(input)
    rate.saveAsObjectFile(output)
  }

  def topNSession(input: String): Unit = {
    val topNCategory = clickActionService.topNCategory(input, 10).map(_._1).toList
    print("=======================")
    topNCategory.foreach(println)
    print("=======================")
    val result = clickActionService.topNSessionByCategory(input, topNCategory, 10)
    result.foreach(println)
  }

}
