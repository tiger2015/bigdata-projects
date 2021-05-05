package com.tiger.scala.traittest

/**
 *
 * @Author Zenghu
 * @Date 2021/3/20 17:13
 * @Description
 * @Version: 1.0
 *
 * */
object TraitDemo1 {

  def main(args: Array[String]): Unit = {
    // 混入特质
    val dog = new Dog("tom") with TraitDemo1
    dog.sayHello(dog.name)
  }
}

class Dog(val name: String) {

}

trait TraitDemo1 {

  def sayHello(name: String): Unit = {
    println("hello, " + name)
  }
}