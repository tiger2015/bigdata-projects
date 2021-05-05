package com.tiger.scala.outerclass

/**
 *
 * @Author Zenghu
 * @Date 2021/3/20 23:06
 * @Description
 * @Version: 1.0
 *
 * */
object Demo1 {

  def main(args: Array[String]): Unit = {
    val outer1 = new ScalaOuterClass
    val outer2 = new ScalaOuterClass
    // 内部类
    val inner1 = new outer1.ScalaInnerClass
    val inner2 = new outer2.ScalaInnerClass
    inner1.show(inner2)
    inner1.show(inner1)
    inner2.show(inner2)
  }

}

class ScalaOuterClass {
  myouter =>

  class ScalaInnerClass {
    def info(): Unit = {
      println("name:" + name + ", salary:" + sal)
      println("name:" + myouter.name + ", salary:" + myouter.sal)
    }

    def show(obj: ScalaOuterClass#ScalaInnerClass): Unit = {
      obj.info()
    }
  }

  var name: String = "tom"
  private val sal = 1000
}

// 静态内部类
object ScalaOuterClass {

  class ScalaStaticInnerClass {
  }

}