package com.tiger.scala

/**
 *
 * @Author Zenghu
 * @Date 2021/3/17 22:48
 * @Description
 * @Version: 1.0
 *
 * */
object ExtendTest {

  def main(args: Array[String]): Unit = {
    val sub = new Sub
    println(sub.age) // 可以直接访问父类
    println(sub.name)
    // println(sub.id) 不可以直接访问
  }

}

class Super {
  var name = "" // 默认访问权限
  var age = 0
  private var id: String = "" // 私有访问权限
}

class Sub extends Super {

  def this(name: String, age: Int) {
    this
    this.name = name
    this.age = age
  }

}
