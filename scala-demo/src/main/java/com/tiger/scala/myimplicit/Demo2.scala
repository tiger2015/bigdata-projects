package com.tiger.scala.myimplicit

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 11:28
 * @Description
 * @Version: 1.0
 *
 * */
object Demo2 {

  // 隐式扩展类的功能
  implicit def f(op: Operate): OperateExtend = {
    new OperateExtend
  }

  def main(args: Array[String]): Unit = {
    val  opt = new Operate
    opt.insert(1)
    opt.delete(2)
  }


}

class Operate {

  def insert(id: Int): Unit = {
    println("insert " + id)
  }
}

class OperateExtend {
  def delete(id: Int): Unit = {
    println("delete " + id)
  }

}