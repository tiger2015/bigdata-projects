package com.tiger.scala.myimplicit

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 11:58
 * @Description
 * @Version: 1.0
 *
 * */
object Demo4 {

  def main(args: Array[String]): Unit = {
    val mySql = new MySql
    mySql.sayHello()
    mySql.sayOk()
  }

  // 1.必须包含一个参数的构造函数；
  // 2.而且参数类型为需要扩展功能的类
  implicit class MyDB(mySql: MySql) {
    def sayOk(): Unit = {
      println("MyDB say Ok")
    }
  }

}

class MySql {

  def sayHello(): Unit = {
    println("MySql say Hello")
  }

}