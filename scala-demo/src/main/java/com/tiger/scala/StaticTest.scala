package com.tiger.scala

/**
 *
 * @Author Zenghu
 * @Date 2021/3/18 22:59
 * @Description
 * @Version: 1.0
 *
 * */
object StaticTest {
  def main(args: Array[String]): Unit = {
    MyStaticObject.count += 1
    MyStaticObject.display()
  }
}

object MyStaticObject {  // 伴生对象
  var count: Int = _

  def display(): Unit = {
    println("count = " + count)
  }
}

class MyStaticObject { // 伴生类
  var name: String = _
}
