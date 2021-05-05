package com.tiger.scala.traittest

/**
 *
 * @Author Zenghu
 * @Date 2021/3/20 9:52
 * @Description
 * @Version: 1.0
 *
 * */
object TraitTest {

  def main(args: Array[String]): Unit = {
    val stu = new Student("Tom")
    stu.sayHello("candy")
    stu.sayHi("candy")

  }

}
// public class Student implements Trait01
class Student extends Trait01 {
  var name: String = _

  def this(inName: String) {
    this
    name = inName
  }

  override def sayHello(name: String): Unit = {
    println("hello, " + name)
  }
}

class Teacher {

}


// 对应接口：interface Trait01
trait Trait01 {

  def sayHello(name: String) // void sayHello(paramString: String);

  def sayHi(name: String): Unit = {  // default void sayHi(String name){} 带有默认实现的方法
    println("hi, " + name)
  }

}