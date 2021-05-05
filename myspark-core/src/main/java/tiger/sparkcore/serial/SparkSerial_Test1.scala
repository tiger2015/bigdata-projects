package tiger.sparkcore.serial

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Author Zenghu
  * @Date 2021/4/14 22:30
  * @Description
  * @Version: 1.0
  *
  **/
object SparkSerial_Test1 {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("serial")

    val sc = new SparkContext(conf)

    val rdd0 = sc.makeRDD(List(1, 2, 3, 4))

    val user = new User("tiger")

    rdd0.foreach(item => {
      println(user.name)
    })


    sc.stop()

  }


//  case class User(var name: String) {
//
//  }


}
class User(var name: String) extends Serializable {

}

class User1(name:String){

}