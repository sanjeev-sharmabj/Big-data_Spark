package pack

object obj {

  def main(args: Array[String]): Unit = {

    println("Started")
    val a = 2
    println(a)
    val b = "zeyobron"
    println(b)
    println("========raw list=============")
    val lsin = List(-100, -21, -30, 4, 5, -6, -5)
    lsin.foreach(println)
    println("=============new list===========")
    val newlsin = lsin.filter(x => x < -20)
    println(newlsin)
    newlsin.foreach(println)
    
    
    val lsstr = List("zeyo","zeyobron","analytics","azeyobron")
    lsstr.foreach(println)
    println("===string filter=====")
    val newstr = lsstr.filter(x => x.contains("zeyo"))
    newstr.foreach(println)
    
    println("=====not condition======")
    val newstr1 = lsstr.filterNot(x => x.contains("abc"))
    newstr1.foreach(println)
  }

}