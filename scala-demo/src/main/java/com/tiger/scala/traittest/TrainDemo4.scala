package com.tiger.scala.traittest

/**
 *
 * @Author Zenghu
 * @Date 2021/3/20 21:38
 * @Description
 * @Version: 1.0
 *
 * */
object TrainDemo4 {

  def main(args: Array[String]): Unit = {

  }

}

trait Empl {
  var name: String;
  val sal: Double;
  var worker: String = "technology"
}

class Engineer extends Empl {
  override var name: String = "tom"
  override val sal: Double = 1000.0
}