package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object may22_rrd_column {
  
  case class sch(id:String,tdate:String,category:String,product:String)
  
  def main (args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("file:///E:/big data/data/datat.txt",1)
    println("==========raw data=============")
    data.foreach(println)
    
    val splitdata = data.map(x => x.split(","))
    
    println("===========imposing column on split data===========")
    
    val schemardd = splitdata.map(x => sch(x(0),x(1),x(2),x(3)))
    
    println("===========filtered data using column name=========")
    
    val fildata = schemardd.filter(x => x.product.contains("Gymnastics"))
    
    fildata.foreach(println)
  }
}