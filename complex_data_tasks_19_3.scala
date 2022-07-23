package spark_june_19_tasks

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object complex_data_array_19_3 {

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
      .load("file:///E:/big data/data/json/random_one_record.json")
    df.show()
    df.printSchema()

    println("===========flattening the file using select===============")

    val explddf = df
      .withColumn(
        "results",
        explode(col("results")))
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

    explddf.persist()
    explddf.show(20, false)
    explddf.printSchema()
    
    println("===========flattening the file using with column===============")

    /*val expldwcdf = df
      .withColumn("results", explode(col("results")))
      .withColumn("user", expr("results.user.*"))
      .withColumn("location", expr("results.user.location.*"))
      .withColumn("name", expr("results.user.name.*"))
      .withColumn("picture", expr("results.user.picture.*"))

    expldwcdf.persist()
    expldwcdf.show(20, false)
    expldwcdf.printSchema()*/

  }

}