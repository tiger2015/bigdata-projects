package tiger.proj01.common

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @Author Zenghu
 * @Date 2021/4/24 23:07
 * @Description
 * @Version: 1.0
 *
 * */
object ApplicationContext {
  var sc: SparkContext = _

  def init(master: String = "local[*]", applicationName: String = "Application") {
    val conf = new SparkConf()
    conf.setAppName(applicationName)
    conf.setMaster(master)
    sc = new SparkContext(conf)
  }


  def close(): Unit = {
    sc.stop()
  }

}







