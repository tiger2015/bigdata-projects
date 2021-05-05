package com.tiger.scala.ovride

/**
 *
 * @Author Zenghu
 * @Date 2021/3/17 23:01
 * @Description
 * @Version: 1.0
 *
 * */
object ScalaOverride {
  def main(args: Array[String]): Unit = {
    val man1:Human = new Man
    println(man1.id)
    println(man1.age())
    val man2 = new Man
    println(man2.id)
    println(man2.age)
  }
}

class Human {
  val id: String = "human"

  def age(): Int = {
    10
  }
}

class Man extends Human {

  override val id: String = "man"

  override val age: Int = 100
}

