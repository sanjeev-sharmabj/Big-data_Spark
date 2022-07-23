package pack

object usecase1 {
  def main(args: Array[String]): Unit = {
    val uclist = List("state->TN~city->Chennai", "state->Gujarat~city->Gandhinagar")
    println("======raw list=============")
    uclist.foreach(println)
    val flatlist = uclist.flatMap(x => x.split("~"))
    println("=======flattened list=========")
    flatlist.foreach(println)
    println("===state=======")
    val statelist = flatlist.filter(x => x.contains("state"))
    statelist.foreach(println)
    println("===city=======")
    val citylist = flatlist.filter(x => x.contains("city"))
    citylist.foreach(println)

    println("===finalstatelist=======")

    val finalstatelist = statelist.map(x => x.replace("state->", ""))
    finalstatelist.foreach(println)

    println("===finalcitylist=======")
    val finalcitylist = citylist.map(x => x.replace("city->", ""))
    finalcitylist.foreach(println)
  }
}