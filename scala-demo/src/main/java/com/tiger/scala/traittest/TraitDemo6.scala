package com.tiger.scala.traittest

/**
 *
 * @Author Zenghu
 * @Date 2021/3/20 22:06
 * @Description
 * @Version: 1.0
 *
 * */
object TraitDemo6 {

  def main(args: Array[String]): Unit = {

  }


}

trait A6 extends Exception {
  def log(): Unit = {
    println(getMessage)
  }
}

class B6 {

}

// 编译错误，因为B6不是Exception的子类
//class C6 extends B6 with A6 {
//
//}