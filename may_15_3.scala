package pack

object may_15_3 {
  def main(args: Array[String]): Unit = {

    val rawlist = List("A~B", "C~D~e~f~G~H")
    println("===raw list=====")
    rawlist.foreach(println)

    val newlist = rawlist.flatMap(x => x.split("~"))
    newlist.foreach(println)
    println("==================")
    val newlist1 = rawlist.flatten(x => x.split("~"))
    newlist1.foreach(println)
    println("==================")

  }
}