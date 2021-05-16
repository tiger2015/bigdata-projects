package tiger

import java.util.Calendar

import tiger.sparkstreaming.proj.entity.AdClick

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 11:09
 * @Description
 * @Version: 1.0
 *
 * */
object Test {

  def main(args: Array[String]): Unit = {

    val date = Calendar.getInstance().getTime

    val ad = new AdClick(date,"华南","广州", "1", "A")

    println(AdClick.toJsonString(ad))


    val json = "{\"area\":\"华南\",\"ad\":\"A\",\"city\":\"广州\",\"clickTime\":\"2021-05-16 11:34:02\",\"user\":\"1\"}"

    println(AdClick.parseFromJson(json).clickTime)



  }

}
