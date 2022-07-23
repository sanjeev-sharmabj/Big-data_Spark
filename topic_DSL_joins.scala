package spark_june_12

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.collection.Seq

object topic_DSL_join {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df1 = spark.
      read.
      format("csv").
      option("header", "true").
      load("file:///E:/big data/data/jn1.txt")

    df1.persist()
    df1.show()

    val df2 = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("file:///E:/big data/data/jn2.txt")

    df2.show()
    df2.persist()

    val df3 = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("file:///E:/big data/data/jn3.txt")
    df3.show()
    df3.persist()

    println("|===========start=============|")
    println("|===========inner join=============|")

    val innerdf = df1.
      join(df2, Seq("id"), "inner")
    innerdf.show()

    println("|===========left join=============|")

    val leftdf = df1.
      join(df2, Seq("id"), "left")
    leftdf.show()

    println("|===========right join=============|")
    val rightdf = df1.
      join(df2, Seq("id"), "right")
    rightdf.show()

    println("|===========left_anti join=============|")
    val antidf = df1.
      join(df2, Seq("id"), "left_anti")
    antidf.show()

    println("|===========outer join=============|")

    val outerdf = df1
      .join(df2, Seq("id"), "outer")
      .orderBy(col("id").asc)
    outerdf.show()

    println("=================left anti join==========")

    val antijoin = df1.
      join(
        df3,
        df1("id") === df3("prod_id"),
        "left_anti")
    antijoin.show()

    println("=================left semi join==========")
    val leftsemi = df1
      .join(df3, df1("id") === df3("prod_id"), "leftsemi")
    leftsemi.show()
    println("==============inner join================")
    val inner = df1
      .join(
        df3,
        df1("id") === df3("prod_id"),
        "inner").
        drop("prod_id")
    inner.show()

    println("================done=================")
  }
}