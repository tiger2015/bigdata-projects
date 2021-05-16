package tiger.sparkstreaming.proj.entity

import java.util.{Calendar, Date}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.annotation.JSONField

import scala.beans.BeanProperty

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 10:58
 * @Description
 * @Version: 1.0
 *
 * */
class AdClick(clickTimeIn: Date, areaIn: String = "", cityIn: String = "", userIn: String = "", adIn: String = "") extends Serializable {

  @JSONField(format = "yyyy-MM-dd HH:mm:ss")
  @BeanProperty var clickTime: Date = clickTimeIn // scala默认生成getter/setter,但是不是getXXX或者setXXX需要使用注解才会有getXX/setXX方法
  @BeanProperty var area: String = areaIn
  @BeanProperty var city: String = cityIn
  @BeanProperty var user: String = userIn
  @BeanProperty var ad: String = adIn

  // fastjson反序列时需要无参构造函数
  def this() {
    // 调用主构造函数
    this(Calendar.getInstance().getTime)
  }

}


object AdClick {

  def toJsonString(adClick: AdClick): String = {
    JSON.toJSON(adClick).toString
  }


  def parseFromJson(json: String): AdClick = {

    JSON.parseObject(json, classOf[AdClick])
  }

}