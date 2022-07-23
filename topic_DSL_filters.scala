package spark_june_05

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object dsl_filters {
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

    println("|===========and operation=============|")
    val filtrdata = df.
      filter(col("country") === "IND"
        && col("spendby") === "cash")
    filtrdata.show()

    println("|=================or operation============|")
    val filtrordata = df.
      filter(col("country") === "IND"
        || col("spendby") === "cash")
    filtrordata.show()

    println("|=========not equal operation===========|")

    val filtrnotdata = df.
      filter(!(col("country") === "IND"))
    filtrnotdata.show()

    println("|=======filter by multiple values and is in operator======|")

    val filtrindata = df.filter(
      (!(col("country") isin ("IND", "US"))) &&
        (!(col("spendby") === ("cash"))))
    filtrindata.show()

    println("|==================like operator=====================|")

    val filtrlikdata = df.filter(
      (col("country") like ("%IN%")) &&
        !(col("country") like ("INDO%")))
    filtrlikdata.show()
  }
}