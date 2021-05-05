package com.tiger.scala.myfunction

/**
 *
 * @Author Zenghu
 * @Date 2021/3/24 22:46
 * @Description
 * @Version: 1.0
 *
 * */
object Demo5 {

  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5)
    println(list.map((x: Int) => x + 1))
    println(list.map((x) => x + 1))
    println(list.map(x => x + 1))
    println(list.map(_ + 1))
  }
}
