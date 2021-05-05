package com.tiger.scala.traittest

/**
 *
 * @Author Zenghu
 * @Date 2021/3/20 17:25
 * @Description
 * @Version: 1.0
 *
 * */
object TraitDemo2 {

  def main(args: Array[String]): Unit = {
    // 创建对象时的执行顺序：从左到右开始执行
    // 编译后
    /*
       Operate.$init$(this);
       Data.$init$(this);
       DB.$init$(this);
       File.$init$(this);
     */
    // 执行结果：
    //  Operate
    //  Data
    //  DB
    // File
    val  mySql = new MySql with File with DB
    // 方法执行的顺序： 从右到左开始执行
    /*  TraitDemo2$$anon$1:
        public void insert(int id) {
            File.insert$(this, id);
         }
       File:
          default void insert(int id) {
              Predef$.MODULE$.println("File insert");
              com$tiger$scala$traittest$File$$super$insert(id);
       }

     */
     // 执行结果：
    //  File insert
    //  DB insert
    //  insert data:1
     mySql.insert(1)
  }

}

class MySql {}


trait Operate {
  println("Operate")

  def insert(id: Int)
}

trait Data extends Operate {
  println("Data")

  override def insert(id: Int) = {
    println("insert data:" + id)
  }
}

trait DB extends Data {
  println("DB")

  override def insert(id: Int): Unit = {
    println("DB insert")
    super.insert(id)
  }
}



trait File_ extends Data {
  println("File_")

  override def insert(id: Int): Unit = {
    println("File_ insert")
    super.insert(id)
  }

}

trait File extends File_ with  Data  {
  println("File")

  override def insert(id: Int): Unit = {
    println("File insert")
    super.insert(id)
  }
}


