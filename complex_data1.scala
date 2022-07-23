package spark_june_18_19

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object complex_data1 {

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
      .load("file:///E:/big data/data/json/jc.json")
    df.show(20, false)
    df.printSchema()
    //using select

    println("==========using select========")
    val flatdf = df.select(
      col("orgname"),
      col("trainer"),
      col("doorno"),
      col("address.permanent_city"),
      col("address.temporary_city"),
      col("address.pincode"))

    flatdf.show()
    flatdf.printSchema()

    //using withcolumn

    println("==========using withcolumn========")

    val df1 = df
      .withColumn(
        "permanent city",
        expr("address.permanent_city"))
      .withColumn(
        "temporary city",
        expr("address.temporary_city"))
      .withColumn(
        "pin_code",
        expr("address.pincode")).drop("address")
    df1.show()
    df1.printSchema()

  }

}