package com.tiger.scala.collections

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 16:04
 * @Description
 * @Version: 1.0
 *
 * */
object Demo1 {

  def main(args: Array[String]): Unit = {
    val list1 = List(1, 2, 3, 4, 5)
    // 执行顺序：
    // 1-2
    // (1-2)-3
    // ((1-2)-3)-4
    // (((1-2)-3)-4)-5
    val list2 = list1.reduceLeft(minus)
    println("======")
    // 执行顺序：
    // 4-5
    // 3-(4-5)
    // 2-(3-(4-5))
    // 1 -(2-(3-(4-5)))
    val list3 = list1.reduceRight(minus)
  }

  def minus(n1: Int, n2: Int) = {
    println(n1 + " - " + n2)
    n1 - n2
  }

}
