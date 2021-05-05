package com.tiger.scala.myfunction

/**
 *
 * @Author Zenghu
 * @Date 2021/3/24 22:07
 * @Description
 * @Version: 1.0
 *
 * */
object Demo4 {
  def main(args: Array[String]): Unit = {
    val d = 36.0
    val  ret = test(sqrt, d)
    println(ret)
    // minus(2) => 2 - y
    println(minus(2))
    println(minus(2)(3))

  }
  // 传递函数f1,传入一个Double,返回一个Double
  def test(f1: Double => Double, d: Double) = {
    f1(d)
  }

  def sqrt(d: Double): Double = {
    math.sqrt(d)
  }

  // 返回一个函数即 x- y
  def minus(x: Double)={
    (y:Double) => x -y
  }
}
