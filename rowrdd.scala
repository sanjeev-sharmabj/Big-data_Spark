package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object rowrdd {
  
  def main (args:Array[String]):Unit={
    
    println("=====started========")
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    println
    println("=======row data=========")
    
    val data = sc.textFile("file:///E:/big data/data/datat.txt")
    data.foreach(println)
    
    val splitdata = data.map(x => x.split(","))
    
    val rowrdd = splitdata.map(x=> Row(x(0),x(1),x(2),x(3)))
    
    println("===========filtered data===============")
    
    val filtrdata = rowrdd.filter(x => x(3).toString().contains("Gymnastics"))
    filtrdata.foreach(println)
    
    val schema = StructType(Array(StructField("id",StringType),
        StructField("tdate",StringType),StructField("category",StringType),StructField("product",StringType)))
        
    println("================dataframe=================")
    
    val df = spark.createDataFrame(filtrdata,schema)
    df.show()
    println
    println("============writing the data as parquet file==========")
    df.write.mode("overwrite").parquet("file:///E:/big data/data/datatparuetrowrdd")
    println
    println("====================done===============")
        
  }
}