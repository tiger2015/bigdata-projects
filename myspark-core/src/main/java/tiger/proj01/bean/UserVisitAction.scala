package tiger.proj01.bean

import tiger.proj01.common.ActionType

class UserVisitAction extends Serializable {

  var date: String = _ // 日期

  var userId: Long = _ // 用户ID

  var sessionId: String = _ // session Id

  var pageId: Long = _ //页面 Id

  var actionTime: String = _ // 动作时间

  var searchKeyWords: String = _ // 搜索的关键词

  var clickCategoryId: Long = _ // 点击的商品类别id

  var clickProductId: Long = _ // 点击的商品id

  var orderCategoryIds: Array[Long] = Array() // 订单所有商品类别的ID

  var orderProductIds: Array[Long] = Array() // 订单中所有商品的ID

  var payCategoryIds: Array[Long] = Array() // 支付的所有商品类别ID

  var payProductIds: Array[Long] = Array() // 支付的所有商品的ID

  var cityId: Long = _ // 城市ID

  var actionType = ActionType.NONE

}
