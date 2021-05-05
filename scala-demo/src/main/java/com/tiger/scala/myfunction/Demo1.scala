package com.tiger.scala.myfunction

/**
 *
 * @Author Zenghu
 * @Date 2021/3/23 22:14
 * @Description
 * @Version: 1.0
 *
 * */
object Demo1 {

  def main(args: Array[String]): Unit = {
    // [Any, Int]: 输入参数类型为Any, 返回类型为Int
    val partialFun = new PartialFunction[Any, Int] {
      override def isDefinedAt(x: Any): Boolean = x.isInstanceOf[Int] // 如果返回true就去调用apply,否则就忽略
      override def apply(v1: Any): Int = {
        v1.asInstanceOf[Int] * v1.asInstanceOf[Int]
      }
    }
    val list = List(1, 2, 3, 4, "a")
    // 如果是偏函数，不能使用map
    val ints = list.collect(partialFun)
    println(ints)


    val ints1 = list.collect({
      case i: Int => i * i
    })
    println(ints1)
  }
}
