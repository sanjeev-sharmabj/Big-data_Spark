package pack1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object sparkobj {
  
  def main (args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("First").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    println("Hello World")
  }
}