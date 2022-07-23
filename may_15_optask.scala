package pack

object may_15_optask {
  def main (args:Array[String]):Unit={
    val list = List("Gymnastics","Cash")
    println("=========raw data==========")
    list.foreach(println)
    
    val gymstr = list.filter(x => x.contains("Gymnastics"))
    println("=====only gym=======")
    gymstr.foreach(println)
  }
}