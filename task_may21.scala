package pack1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object task_may21 {
  
  def main (args:Array[String]):Unit={  
  val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("file:///E:/big data/data/txns")
    println("====printing raw data==========")
    data.foreach(println)
    println
    println(data.count())
    println("============================")
    println
    println("=========filtering Gymnastics Data=============")
    val gymdata = data.filter(x => x.contains("Gymnastics"))
    gymdata.foreach(println)
    println(gymdata.count())
  }
  
}