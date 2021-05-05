package com.tiger.scala.mymatch

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 21:29
 * @Description
 * @Version: 1.0
 *
 * */
object Demo6 {

  def main(args: Array[String]): Unit = {
    val (x, y) = (1, "hello") // x=1, y = "hello"
    val (q, r) = BigInt(100) /% 3 // q= BigInt(100) /3 r= BigInt(100)%3 注意：'%'不能放在'/'前面
    println(q + ", " + r)
    val arr = Array(1, 2, 3, 4)
    val Array(first, second, _*) = arr // 提取前两个元素
    println(first + ", " + second)
  }

}
