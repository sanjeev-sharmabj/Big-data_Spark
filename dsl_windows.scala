package spark_june_12

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._

object dsl_windows {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("file:///E:/big data/data/dept.txt")
    df.show()
    df.persist()

    val deptpart = Window
      .partitionBy("department")
      .orderBy(col("salary").desc)
    println("==============second highest salary===========")
    df
      .withColumn("rank", dense_rank()
        .over(deptpart))
      .filter("rank=2")
      .show()

  }
}