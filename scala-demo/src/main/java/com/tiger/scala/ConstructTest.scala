package com.tiger.scala

/**
 *
 * @Author Zenghu
 * @Date 2021/3/14 21:28
 * @Description
 * @Version: 1.0
 *
 * */
object ConstructTest {

  def main(args: Array[String]): Unit = {
    val p = new Person // 先调用主构造器，再调用辅助构造器
    println(p.inAge) // inAge只可以读
    // p.inAge = 10
    p.inName = "mary" // inName可读可写
    println(p.inName)
  }

}

class Person(var inName: String, val inAge: Int) {
  var name: String = inName
  var age = inAge
  println("main constructor")

  def this() = {
    this("tom", 12)
    println("assistance constructor")
  }
}