package com.tiger.scala.myfunction

/**
 *
 * @Author Zenghu
 * @Date 2021/3/24 22:53
 * @Description
 * @Version: 1.0
 *
 * */
object Demo6 {

  def main(args: Array[String]): Unit = {
    println(minus1(10,2)) // 传递多个参数
    // minus2(10) 返回函数：10-y
    println(minus2(10)(2)) // 多个函数，每个函数传递一个参数

    val s="Hello"
    println(s.checkEq("Hell1")(eq))
  }

  def minus1(x:Int, y:Int) = x - y
  def minus2(x: Int) = (y: Int) => x - y

  def eq(s:String, o:String) ={s.equals(o)}

  // 隐式类
  implicit class TestEq(s:String){
     // 定义函数checkEq
    // 参数：o:String
    // 函数f: 输入两个String,返回一个Boolean
     def checkEq(o:String)(f:(String,String)=>Boolean)={
       f(s.toLowerCase, o.toLowerCase())
     }
  }
}

