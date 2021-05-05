package com.tiger.scala.mymatch

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 21:00
 * @Description
 * @Version: 1.0
 *
 * */
object Demo2 {

  def main(args: Array[String]): Unit = {
    val a = 5
    val obj = if (a == 1) "hello" else if (a == 2) 1 else if (a == 3) Map("hello" -> a)
    else if (a == 4) Map(a -> "hello") else Array(1)

    val ret = obj match {
      case a: String => a
      case b: Int => b
      case c: Map[String, Int] => c
      case d: Map[Int, String] => d
      case e: Array[Int] => e
      case _ => "error"
    }
    println(ret)

  }
}
