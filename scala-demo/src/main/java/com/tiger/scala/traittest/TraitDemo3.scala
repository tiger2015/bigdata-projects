package com.tiger.scala.traittest

/**
 *
 * @Author Zenghu
 * @Date 2021/3/20 18:15
 * @Description
 * @Version: 1.0
 *
 * */
object TraitDemo3 {

  def main(args: Array[String]): Unit = {

  }

}

trait Operate3 {
  def sayHello(name: String)
}

trait File3 extends Operate3 {

  abstract override  def sayHello(name: String): Unit = {
    println("file sayHello Hello " + name)
    super.sayHello(name)
  }

}
