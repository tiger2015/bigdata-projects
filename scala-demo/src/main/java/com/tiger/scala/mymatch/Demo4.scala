package com.tiger.scala.mymatch

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 21:17
 * @Description
 * @Version: 1.0
 *
 * */
object Demo4 {
  def main(args: Array[String]): Unit = {
    val array = List(List(0), List(1, 0), List(0, 1, 0), List(0, 0, 0), List(1, 0, 0))
    for (arr <- array) {
      val ret = arr match {
        case 0 :: Nil => "0" // 只有一个元素0
        case x :: y :: Nil => x + ", " + y // 只有两个元素
        case 0 :: tail => "0...." // 以0开头
        case _ => "other"
      }
      println(ret)
    }
  }

}
