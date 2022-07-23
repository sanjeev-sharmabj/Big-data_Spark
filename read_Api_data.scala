package spark_june_25_26

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.io.Source

object read_API_data {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val urldata = Source
      .fromURL("https://randomuser.me/api/0.8/?results=5").mkString
    println(urldata)
    println("=====converting json string to rdd=======")
    val rdd = sc.parallelize(List(urldata))
    println("=======converting rdd to data frame=======")

    val flatdf = spark.read.json(rdd)
    flatdf.show()
    flatdf.printSchema()

    println("=========flattening the json========")

    val flatjson = flatdf
      .withColumn("results", explode(col("results")))
      .select(
        col("nationality"),
        col("results.user.*"),
        col("results.user.location.*"),
        col("results.user.name.*"),
        col("results.user.picture.*"),
        col("seed"),
        col("version"))
      .drop("location")
      .drop("name")
      .drop("picture")
      
      flatjson.show()
      flatjson.printSchema()
      
     println("===========writing the data as csv========")
     
     flatjson.
     write.
     format("csv").
     option("header","true")
     .partitionBy("nationality")
     .mode("append")
     .save("file:///E:/big data/data/urldata")
     

  }

}