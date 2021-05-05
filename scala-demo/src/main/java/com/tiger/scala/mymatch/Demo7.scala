package com.tiger.scala.mymatch

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 21:35
 * @Description
 * @Version: 1.0
 *
 * */
object Demo7 {
  def main(args: Array[String]): Unit = {
    val map = Map("A" -> 1, "B" -> 0, "C" -> 3)
    for ((k, v) <- map) {
      println(k + "->" + v)
    }
    println("==============")
    for ((k, 0) <- map) { // 匹配value是0的元素
      println(k + "->" + 0)
    }
  }


}
