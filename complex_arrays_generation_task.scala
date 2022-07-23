package spark_june_25_26

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object complex_arrays_generation_task {

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
      .load("file:///E:/big data/data/json/actors.json")
    df.show(20, false)
    df.printSchema()

    println("===========flattening the file==========")

    val flattendf = df.
      withColumn("actors", explode(col("actors")))
      .withColumn("childrens", explode(col("Actors.children")))
      .select(
        col("actors.*"),
        col("childrens"),
        col("country"),
        col("version")).drop("Children")
      .withColumnRenamed("childrens", "children")

    flattendf.show()
    flattendf.printSchema()

    println("===============reverting the array==========")

    val revarray = flattendf
      .groupBy(
        col("Birthdate"),
        col("Born At"),
        col("BornAt"),
        col("age"),
        col("hasChildren"),
        col("hasGreyHair"),
        col("name"),
        col("photo"),
        col("weight"),
        col("wife"),
        col("country"),
        col("version")).agg(collect_list("children").alias("children"))
        
        revarray.show()
        revarray.printSchema()

    val finalrevarray = revarray
      .groupBy(col("country"), col("version"))
      .agg(collect_list(struct(
        col("Birthdate"),
        col("Born At"),
        col("BornAt"),
        col("age"),
        col("children"),
        col("hasChildren"),
        col("hasGreyHair"),
        col("name"),
        col("photo"),
        col("weight"),
        col("wife"))).alias("Actors"))
      .select("Actors", "country", "version")

    finalrevarray.show(20, false)
    finalrevarray.printSchema()

  }

}