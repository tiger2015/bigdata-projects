package com.tiger.scala

/**
 *
 * @Author Zenghu
 * @Date 2021/3/14 17:45
 * @Description
 * @Version: 1.0
 *
 * */
object FunTest2 {

  def main(args: Array[String]): Unit = {
    def display() {
      println("hello2")

      def display() {
        println("hello3")
      }
    }

    display
  }

  def display() {
    println("hello1")
  }

}
