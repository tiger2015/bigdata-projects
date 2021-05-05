package com.tiger.scala

import scala.util.control.Breaks.{break, breakable}

/**
 *
 * @Author Zenghu
 * @Date 2021/3/13 16:31
 * @Description
 * @Version: 1.0
 *
 * */
object TypeTest {

  def main(args: Array[String]): Unit = {
    var f1 = 0.99876555633F
    println(f1)

    var ch1: Char = 'a'
    var ch2: Char = 97
    // var ch3:Char = 97 +1
    // var ch4:Char = 'a'+1
    var a = 10
    println(change(a))
    10 max 2
    10.max(2)

    var s1: Short = 90
    var b1: Byte = 12
    println(12.3.toInt)
    println(-2.5.toInt)


    var ++ = "hello"
    println(++)

    var +- = "hello"
    println(+-)

    var `true` = true
    println(`true`)

    var i2: Int = 12
    var s = i2 + ""

    var s2 = "12.5"
    //println(s2.toInt)


    println(s2 * 3)

    var d = 10

    println(2 >> 2) // 0
    println(-2 >> 2) // -1

    for (i <- 1 until 10 if i != 3)
      println(i)

    for (i <- 1 until 10; j = i * 3)
      println(j)

    for (i <- 1 until 10; j <- 1 until 3)
      println("i=" + i + ", j=" + j)

    breakable {
      var n = 1
      while (n < 20) {
        n += 1
        if (n > 18) {
          break()
        }
      }
    }


  }


  def change(a: Int): Unit = {
    a * 10
  }
}
