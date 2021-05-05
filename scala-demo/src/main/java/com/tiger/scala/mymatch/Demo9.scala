package com.tiger.scala.mymatch

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 22:35
 * @Description
 * @Version: 1.0
 *
 * */
object Demo9 {

  def main(args: Array[String]): Unit = {
    val array = Array(Dollar(1000), Currency(2000, "￥"), NoAmount)
    for (arr <- array) {
      val ret = arr match {
        case Dollar(x) => "$" + x
        case Currency(x, y) => x + "" + y
        case NoAmount => ""
      }
      println(ret)
    }
  }
}

abstract class Amount

case class Dollar(value: Double) extends Amount // 样例类

case class Currency(value: Double, unit: String) extends Amount

case object NoAmount extends Amount