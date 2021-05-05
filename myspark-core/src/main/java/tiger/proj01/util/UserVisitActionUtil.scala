package tiger.proj01.util

import tiger.proj01.bean.UserVisitAction
import tiger.proj01.common.ActionType

/**
 *
 * @Author Zenghu
 * @Date 2021/4/18 21:58
 * @Description
 * @Version: 1.0
 *
 * */
object UserVisitActionUtil extends Serializable {

  def parseClickLog(log: String): UserVisitAction = {

    val splits = log.split("_")

    val userAction = new UserVisitAction()

    userAction.date = splits.apply(0)
    userAction.userId = splits.apply(1).toLong
    userAction.sessionId = splits.apply(2)
    userAction.pageId = splits.apply(3).toLong
    userAction.actionTime = splits.apply(4)
    userAction.cityId = splits.apply(splits.length - 1).toLong
    if (!splits.apply(5).equals("null")) {
      userAction.actionType = ActionType.SEARCH
      userAction.searchKeyWords = splits.apply(5)
    }

    userAction.clickCategoryId = splits.apply(6).toLong
    userAction.clickProductId = splits.apply(7).toLong
    if (userAction.clickProductId != -1) {
      userAction.actionType = ActionType.CLICK
    }

    if (!splits.apply(8).equals("null")) {
      userAction.orderCategoryIds = splits.apply(8).split(",").map(_.toLong)
    }

    if (!splits.apply(9).equals("null")) {
      userAction.orderProductIds = splits.apply(9).split(",").map(_.toLong)
      userAction.actionType = ActionType.ORDER
    }

    if (!splits.apply(10).equals("null")) {
      userAction.payCategoryIds = splits.apply(10).split(",").map(_.toLong)
    }

    if (!splits.apply(11).equals("null")) {
      userAction.payProductIds = splits.apply(11).split(",").map(_.toLong)
      userAction.actionType = ActionType.PAY
    }
    userAction
  }


}
