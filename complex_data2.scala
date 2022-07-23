package spark_june_18_19

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object complex_data2 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val df = spark.
      read.
      format("json")
      .option("multiline", "true")
      .load("file:///E:/big data/data/json/donut.json")
    df.show()
    df.printSchema()
    //using select

    println("==========using select========")
    val flatdf = df.select(
      col("id"),
      col("image.height").alias("image_height"),
      col("image.url").alias("image_url"),
      col("image.width").alias("image_width"),
      col("name"),
      col("thumbnail.height").as("thumb_height"),
      col("thumbnail.url").as("thumb_url"),
      col("type"))

    flatdf.show()
    flatdf.printSchema()
    println("=============filtering the data ======")
    val filtrdata = flatdf.
      filter(col("image_height") === 200)
    filtrdata.show()

    //using withcolumn

    println("==========using withcolumn========")
    val withcoldf = df
      .withColumn("image_height", expr("image.height"))
      .withColumn("image_url", expr("image.url"))
      .withColumn("image_width", expr("image.width"))
      .withColumn("image_height", expr("image.height"))
      .withColumn("thumb_height", expr("thumbnail.height"))
      .withColumn("thumb_url", expr("thumbnail.url"))
      .drop("image")
      .drop("thumbnail")
    withcoldf.show()
    withcoldf.printSchema()
  }

}