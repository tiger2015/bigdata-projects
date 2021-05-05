package com.tiger.scala.mymatch

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 21:10
 * @Description
 * @Version: 1.0
 *
 * */
object Demo3 {

  def main(args: Array[String]): Unit = {
    val array = Array(Array(0), Array(1, 0), Array(0, 1, 0), Array(1, 1, 0), Array(1, 1, 1, 0))
    for (arr <- array) {
      val ret = arr match {
        case Array(0) => "0"
        case Array(x, y) => x + "," + y
        case Array(x, y, z) => x + "," + y + "," + z
        case Array(0, _*) => "以0开头的数组"
        case _ => "other"
      }
      println(ret)
    }
  }
}
