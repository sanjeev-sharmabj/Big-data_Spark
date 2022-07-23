package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object sparkobject2 {
  def main (args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    println("================second project===========")
  }
}