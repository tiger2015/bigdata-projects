package com.tiger.scala

/**
 *
 * @Author Zenghu
 * @Date 2021/3/14 17:52
 * @Description
 * @Version: 1.0
 *
 * */
object FunTest3 {

  def main(args: Array[String]): Unit = {
    show("tom", 24) // name:tom	age=24	salary:5000.00
    show("jack", 23, 6000) // name:jack	age=23	salary:6000.00
  }


  def show(name: String, age: Int, sal: Double = 5000) {
    printf("name:%s\tage=%d\tsalary:%.2f\n", name, age, sal)
  }


}
