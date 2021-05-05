package com.tiger.scala.mymatch

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 20:50
 * @Description
 * @Version: 1.0
 *
 * */
object Demo1 {

  def main(args: Array[String]): Unit = {
    val opt = 'a'
    val n1 = 10
    val n2 = 20
    val ret = opt match {
      case '-' => n1 - n2
      case '+' => n1 + n2
      case '*' => n1 * n2
      case '/' => n1 / n2
      case _ if(opt >= 48 && opt <= 57) => println("number") // 守卫条件,不表示默认匹配
      case _ => println("error") // 返回类型为Unit
    }
    println(ret)
  }


}
