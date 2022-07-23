package spark_june_19_tasks

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object complex_data_array_19_2 {

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
      .load("file:///E:/big data/data/json/place.json")
    df.show(20, false)
    df.printSchema()
    println("============flattening the file==========")
    val strdf = df
      .select(
        col("place"),
        col("user.address.*"),
        col("user.name"))

    strdf.show()
    strdf.printSchema()

  }

}