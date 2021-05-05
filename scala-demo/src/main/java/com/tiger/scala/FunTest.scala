package com.tiger.scala

/**
 *
 * @Author Zenghu
 * @Date 2021/3/14 17:41
 * @Description
 * @Version: 1.0
 *
 * */
object FunTest {


  def main(args: Array[String]): Unit = {
    println(sum1(1, 2)) // 3
    println(sum2(1, 2)) // （）
    println(sum3(1, 2)) // 3

  }

  def sum1(n1: Int, n2: Int): Int = { // 指定返回类型
    n1 + n2
  }

  def sum2(n1: Int, n2: Int) { // 返回类型为Unit, 返回语句不生效
    n1 + n2
  }

  def sum3(n1: Int, n2: Int) = { // 返回类型自动推导
    n1 + n2
  }


}
