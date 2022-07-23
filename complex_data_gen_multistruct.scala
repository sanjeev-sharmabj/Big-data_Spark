package spark_june_18_19

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object complex_data_gen_multistruct {

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

    //flattening the file

    println("==========flattening the file========")

    val flatdf = df.select(
      col("id"),
      col("name"),
      col("type"),
      col("image.height").alias("iheight"),
      col("image.url").alias("iurl"),
      col("image.width").alias("iwidth"),
      col("thumbnail.height").alias("theight"),
      col("thumbnail.url").alias("turl"),
      col("thumbnail.width").alias("twidth"))

    flatdf.persist()
    flatdf.show(20, false)
    flatdf.printSchema()

    println("=================done==============")

  }

}