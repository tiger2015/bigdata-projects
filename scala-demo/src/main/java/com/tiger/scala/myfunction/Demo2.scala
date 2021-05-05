package com.tiger.scala.myfunction

/**
 *
 * @Author Zenghu
 * @Date 2021/3/23 22:38
 * @Description
 * @Version: 1.0
 *
 * */
object Demo2 {

  def main(args: Array[String]): Unit = {
    def plus(x: Int) = x + 4

    val array = Array(1, 2, 3, 4)

    val ret = array.map(plus(_))
    // 简写
    array.map(plus)
    println("function type:" + (plus _))
    println(ret.mkString(","))
  }

}
