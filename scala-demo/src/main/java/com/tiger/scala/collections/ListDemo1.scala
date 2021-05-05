package com.tiger.scala.collections

/**
 *
 * @Author Zenghu
 * @Date 2021/3/21 18:24
 * @Description
 * @Version: 1.0
 *
 * */
object ListDemo1 {

  def main(args: Array[String]): Unit = {
    val list = List() // 不可变集合
    val list2 = list :+ 1 // 添加一个元素
    println(list2) // List(1)

    // :: 向集合中添加多个元素
    val list3 = 4 :: 5 :: list2
    println(list3) // List(4, 5, 1)
    // Nil是空列表
    val list4 = 4 :: 5 :: Nil
    println(list4) // List(4, 5)

    val list5 = 7 :: 8 :: list4 :: 9 :: Nil
    println(list5) // List(7, 8, List(4, 5), 9)

    val list6 = 7 :: 8 :: list4 ::: Nil // 将list4中的所有元素加入到集合
    println(list6) // List(7, 8, 4, 5)
    val list7 = 7 :: 8 :: list4 :: Nil // 将list4作为一个元素加入到集合
    println(list7) //List(7, 8, List(4, 5))
  }
}
