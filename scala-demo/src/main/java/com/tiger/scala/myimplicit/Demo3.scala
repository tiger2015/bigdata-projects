package com.tiger.scala.myimplicit

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 11:37
 * @Description
 * @Version: 1.0
 *
 * */
object Demo3 {
  implicit val name: String = "tony" // 隐式值
  implicit val age: Int = 11
  def main(args: Array[String]): Unit = {
    sayHello // 使用隐式值
  }

  // 1. 隐式参数implicit关键字不能省略，否则不能使用隐式参数
  // 2. 函数参数要么全部用隐式值，要么都不用隐式值
  def sayHello(implicit name: String, age: Int) = {
    println("hello " + name + " age:" + age)
  }

}
