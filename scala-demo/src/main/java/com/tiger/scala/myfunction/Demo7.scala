package com.tiger.scala.myfunction

/**
 *
 * @Author Zenghu
 * @Date 2021/3/29 23:14
 * @Description
 * @Version: 1.0
 *
 * */
object Demo7 {

  def main(args: Array[String]): Unit = {
    val list = List("a", "b", "c")
    val tuples: List[(String, Int)] = list.map((s) => {
      (s, 1)
    })
    println(tuples)

    val functions = list.map(S => {
      (S, 1)
    })

    println(functions)
    val list1 = List(1,2,3,4)
    list1.reduce(_ + _)

  }

}
