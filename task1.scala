package pack

object task1 {
  def main(args: Array[String]): Unit = {

    val lsin = List(1, 2, 3, 4)
    println("=========actual list=====")
    lsin.foreach(println)

    println("=====multipled list==========")

    val mullis = lsin.map(x => x * 2)
    mullis.foreach(println)
  }
}