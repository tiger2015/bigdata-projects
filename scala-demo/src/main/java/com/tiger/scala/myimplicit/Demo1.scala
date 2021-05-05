package com.tiger.scala.myimplicit

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 11:20
 * @Description
 * @Version: 1.0
 *
 * */
object Demo1 {

  def main(args: Array[String]): Unit = {

    implicit def f(d: Double) = {
      d.toInt
    }
    val i:Int = 123.5
    println(i)
  }


}
