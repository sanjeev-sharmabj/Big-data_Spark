package spark_june_05

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object topic_DSL_expressions {
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
    val procdata = df.selectExpr(
      "id",
      "split(tdate,'-')[2] as year",
      "initcap(name) as name",
      "check",
      "spendby",
      "UPPER(country)",
      "case when spendby='cash' then 0 else 1 end as status")
    procdata.show()
    
    println("================done=================")
  }
}