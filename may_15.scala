package pack

object may_15 {

  def main(args: Array[String]): Unit = {
    val lsstr = List("zeyo", "zeyobron", "analytics", "azeyobron", "azeoy", "Zeyo")
    println("===raw list=====")
    lsstr.foreach(println)
    println("===string filter=====")
    val newstr = lsstr.filter(x => x.contains("zeyo"))
    newstr.foreach(println)

    val statcitystr = List(
      "State--> Karnataka",
      "City--> Bangalore",
      "State--> Telangana",
      "City--> Hyderabad")
    println("==================raw list=================")
    statcitystr.foreach(println)
    println("=====state============")
    val state = statcitystr.filter(x => x.contains("State"))
    state.foreach(println)
    println("======city======")
    val city = statcitystr.filter(x => x.contains("City"))
    city.foreach(println)

  }
}