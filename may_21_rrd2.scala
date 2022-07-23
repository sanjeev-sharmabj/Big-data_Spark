package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object may_21_rrd2 {
  
  def main (args:Array[String]):Unit={
    
    println("========Start========")
    
    val Conf = new SparkConf().setAppName("First").setMaster("local[*]")
    val sc = new SparkContext(Conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("file:///E:/big data/data/usdata.csv")
    println("============raw data=============")
    data.take(10).foreach(println)
    
    println
    println("================len data===========")
    println
    
    val lenfilt = data.filter(x => x.length()>200)
    lenfilt.foreach(println)
    
    println
    println("================flatmap with delimited, data===========")
    println
    
    val flatdata = lenfilt.flatMap(x => x.split(","))
    flatdata.foreach(println)
    
    println
    println("================sufix data with zeyo===========")
    println
    
    val name = ",zeyo"
    
    val sufdata = flatdata.map(x => x+name)
    sufdata.foreach(println)
    
    println
    println("================replace - data with nothing ===========")
    println
    
    val repdata = sufdata.map(x => x.replace("-", ""))
    repdata.foreach(println)
    
    println
    println("================writing data to file===========")
    println
    
    repdata.coalesce(1).saveAsTextFile("file:///E:/big data/data/repdata")
    println("============done===============")
  }
}