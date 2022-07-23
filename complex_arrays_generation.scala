package spark_june_25_26

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object complex_arrays_generation {

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
      .load("file:///E:/big data/data/json/jc3.json")
    df.show()
    df.printSchema()

    val arraydf = df.
      withColumn(
        "Students_names",
        explode(col("Students"))).drop("Students")
    arraydf.show()
    arraydf.printSchema()
    println("==========reverting the array============")
    val arrayrevdf = arraydf
      .groupBy("doorno", "orgname", "trainer")
      .agg(collect_list("Students_names").alias("students"))

    arrayrevdf.show()
    arrayrevdf.printSchema()

  }

}