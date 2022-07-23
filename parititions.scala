package spark_june_04

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object partitions {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val tschema = StructType(Array(
      StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("check", StringType, true),
      StructField("spendby", StringType, true),
      StructField("country", StringType, true)))

    val fread = spark.
      read.
      format("csv").
      schema(tschema).
      load("file:///E:/big data/data/allcountry.csv")

    fread.show()
    val df1 = fread.select("id","name" ,"check")
    fread.write.format("csv").
      option("header", "true").
      mode("overwrite").
      partitionBy("country", "spendby").
      save("file:///E:/big data/data/countrydf")

  }
}