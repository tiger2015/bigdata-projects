package com.tiger.scala.mymatch

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 21:59
 * @Description
 * @Version: 1.0
 *
 * */
object Demo8 {

  def main(args: Array[String]): Unit = {
    val number = Square.apply(10)
    println("number=" + number)

    val ret = number match {
      case Square(x) => "匹配x=" + x
      case _ => "未匹配"
    }
    println(ret)
  }
}

object Square {
  // unapply:对象提取器
  // 参数为x: Double
  // 返类型：Option[Double]
  def unapply(x: Double): Option[Double] = {
    Some(Math.sqrt(x))
    //None // 不匹配
  }

  def apply(x: Double): Double = x * x
}
