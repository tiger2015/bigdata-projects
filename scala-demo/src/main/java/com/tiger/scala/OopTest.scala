package com.tiger.scala

/**
 *
 * @Author Zenghu
 * @Date 2021/3/14 17:58
 * @Description
 * @Version: 1.0
 *
 * */
object OopTest {

  def main(args: Array[String]): Unit = {
    var dog = new Dog
    println(dog.color)
    println(dog.weight)
    println(dog.name)
    println(dog.age)

  }
}

class Cat {
  var color: String = ""
  var weight: Double = .5
  var age: Int = _
}

class Dog {
  var color: String = _ // 默认值为null
  var weight: Double = _ // 默认值为0.0
  var name: String = _ // 默认值为null
  var age: Int = _ // 默认值为0
}