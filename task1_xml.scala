package spark_june_04

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object task1_xml {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
    setAppName("first").
    setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val xmlread = spark.
      read.
      format("xml").
      option("rowTag", "POSLog").
      load("file:///E:/big data/data/transactions.xml")
    xmlread.show(20,false)
    xmlread.printSchema()

  }
}