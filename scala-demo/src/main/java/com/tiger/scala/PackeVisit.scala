/**
 *
 * @Author Zenghu
 * @Date 2021/3/15 23:10
 * @Description
 * @Version: 1.0
 *
 * */

package com.tiger.scala.visit

class Person {
    private var name = ""
    var age: Int = 0
    private[visit] var id="" // 扩大访问权限
  }

  object PackeVisit {

    def main(args: Array[String]): Unit = {
      var p = new Person
      // 无法访问p.name
      p.age = 10 // age可读可写
      p.id = "1" // 扩大访问权限后
    }
  }



