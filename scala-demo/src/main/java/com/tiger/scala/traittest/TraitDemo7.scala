package com.tiger.scala.traittest

/**
 *
 * @Author Zenghu
 * @Date 2021/3/20 22:14
 * @Description
 * @Version: 1.0
 *
 * */
object TraitDemo7 {

  def main(args: Array[String]): Unit = {
    val console = new Console

  }
}

trait Logger {
  // 告诉编译我是Exception
  this: Exception =>
  {
    println(getMessage)
  }

}

class Console extends Exception with Logger {

}