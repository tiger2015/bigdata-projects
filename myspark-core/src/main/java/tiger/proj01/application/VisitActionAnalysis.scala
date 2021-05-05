package tiger.proj01.application

import tiger.proj01.common.ApplicationContext
import tiger.proj01.controller.UserVisitActionAnalysisController


/**
 *
 * @Author Zenghu
 * @Date 2021/4/25 21:23
 * @Description
 * @Version: 1.0
 *
 * */
object VisitActionAnalysis {

  def main(args: Array[String]): Unit = {
    ApplicationContext.init()
    val path = "data/user_visit_action.txt"
    val output = "data/jump_rate"

    val clickActionAnalysis = new UserVisitActionAnalysisController()

    //clickActionAnalysis.topNCategory(path, 10)
    //clickActionAnalysis.pageJumpRate(path, output)

    clickActionAnalysis.topNSession(path)

    ApplicationContext.close()
  }


}
