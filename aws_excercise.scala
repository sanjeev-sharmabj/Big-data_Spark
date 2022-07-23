package spark_june_12

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._

object aws_excercise {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("first")
      .setMaster("local[*]")
      .set(
        "fs.s3a.access.key",
        "AKIA2RDQ4DHZL6CICLEO")
      .set(
        "fs.s3a.secret.key",
        "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")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("s3a://com.zeyo.dev/txns10k.txt")
    df.show()

    val df1 = spark
      .read
      .format("json")
      .option("header", "true")
      .load("s3a://com.zeyo.dev/devices.json")
      
      df1.show()
  }
}