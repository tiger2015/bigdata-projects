package com.tiger.scala.mymatch

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 21:23
 * @Description
 * @Version: 1.0
 *
 * */
object Demo5 {
  def main(args: Array[String]): Unit = {
    val array = Array((1, 0), (1, 0), (1, 1, 1))
    for (arr <- array) {
      val ret = arr match {
        case (0, _) => "0...." // 第一个元素为0
        case (y, 0) => y // 第二个元素是0
        case (x, y) => (y, x) // 匹配两个元素
        case _ => "other"
      }
      println(ret)

    }
  }


}
