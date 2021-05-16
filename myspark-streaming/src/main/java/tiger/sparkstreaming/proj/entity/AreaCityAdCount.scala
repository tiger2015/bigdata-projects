package tiger.sparkstreaming.proj.entity

import java.util.{Calendar, Date}

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 17:24
 * @Description
 * @Version: 1.0
 *
 * */
class AreaCityAdCount(var date: Date, var area: String, var city: String, var ad:String, var count: Int) {

  def this() {
    this(Calendar.getInstance().getTime, "", "", "", 0)
  }

}
