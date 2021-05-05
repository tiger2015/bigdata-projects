package com.tiger.scala.traittest

/**
 *
 * @Author Zenghu
 * @Date 2021/3/20 21:46
 * @Description
 * @Version: 1.0
 *
 * */
object TraitDemo5 {

  def main(args: Array[String]): Unit = {
    // 方式2
    val g = new G with C with D



  }

}

trait A {
  println("A====")
}

trait B extends A {
  println("B====")
}

trait C extends B {
  println("C=====")
}

trait D extends B {
  println("D=====")
}

class E {
  println("E====")
}

class F extends E with C with D {
  println("F=====")
}

class G extends E {
  println("G====")
}