package pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import sys.process._

object may_21 {
  def main (args:Array[String]):Unit={
    println("=========started===========")
    val Conf = new SparkConf().setAppName("First").setMaster("local[*]")
    val sc = new SparkContext(Conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("file:///E:/big data/data/datatxns.txt")
    println("========raw data=================")
    data.foreach(println)
    
    val gymdata = data.filter(x => x.contains("Gymnastics"))
    .map(x => "this is "+x+" --->end of the line").filter(x => x.contains("Milwaukee"))
    
    println("=====proc data======")
    gymdata.foreach(println)
  }
}