package spark_june_11

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object topic_DSL_expressions_continued {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.
      read.
      format("csv").
      option("header", "true").
      load("file:///E:/big data/data/allcountry_updated.csv")
    df.persist()
    df.show()

    println("|===========start=============|")
    println("|===========expressions=============|")
    val procdata1 = df
      .withColumn("tdate", expr("split(tdate,'-')[2]"))
    procdata1.show()
    val procdata2 = procdata1
      .withColumnRenamed("tdate", "year")
    procdata2.show()
    val procdata3 = procdata2
      .withColumn("status", expr("case when spendby = 'cash' then 1 else 0 end"))
    procdata3.show()
    
    procdata3.printSchema()

    println("================done=================")
  }
}