package pack

object may_15_2 {
  def main(args: Array[String]): Unit = {

    val lsstr = List("zeyo", "zeyobron", "analytics", "azeyobron", "azeoy", "Zeyo")
    println("===raw list=====")
    lsstr.foreach(println)
    println("===updated list=====")
    val constr = lsstr.map(x => x + ",Sairam")
    constr.foreach(println)
    println("===updated list2=====")
    val constr1 = lsstr.map(x => "Om-" + x + "-sairam")
    constr1.foreach(println)

    println("===update list3=====")

    val constr2 = lsstr.map(x => x.concat("_Saibaba"))
    constr2.foreach(println)

  }
}