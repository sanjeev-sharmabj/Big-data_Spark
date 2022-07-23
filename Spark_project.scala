package com.project.spark.spark_project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.time.{ ZonedDateTime, ZoneId }

object Spark_project {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    /*val today = DateTimeFormatter
      .ofPattern("yyyy-MM-dd")
      .format(LocalDate.now())
    */

    val today = DateTimeFormatter
      .ofPattern("yyyy-MM-dd")
      .format(ZonedDateTime.now(ZoneId.of("UTC")))

    /*val today = ZonedDateTime.now(ZoneId.of("UTC"))
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val result = formatter.format(today)*/

    println("<=========reading avro file==========>")

    val avrodf = spark
      .read
      .format("com.databricks.spark.avro")
      .load(s"file:///E:/big data/data/avrofiles/$today")
    avrodf.show()
    avrodf.printSchema()

    //reading data from URL

    val urldata = Source
      .fromURL("https://randomuser.me/api/0.8/?results=500")
      .mkString

    //converting the json string to rdd

    val urlrdd = sc
      .parallelize(List(urldata))

    println("<=========converting rdd to dataframe==========>")

    val urldf = spark.read.json(urlrdd)
    urldf.show()
    urldf.printSchema()

    val flattenurldf = urldf
      .withColumn("results", explode(col("results")))
      .select(
        col("nationality"),
        col("results.*"),
        col("results.user.*"),
        col("results.user.location.*"),
        col("results.user.name.*"),
        col("results.user.picture.*"),
        col("seed"),
        col("version"))
      .drop("user", "location", "name", "picture")

    flattenurldf.show()
    flattenurldf.printSchema()

    println("<=========removing numeric from username column==========>")

    val updatedurldf = flattenurldf
      .withColumn("username", regexp_replace(col("username"), "[0-9]", ""))
    updatedurldf.show()
    updatedurldf.printSchema()

    println("<=========left join of two dataframes==========>")

    val joineddf = avrodf
      .join(updatedurldf, Seq("username"), "left")
    joineddf.show()

    println("<=========nationality column without nulls==========>")

    val wonulldf = joineddf.filter(col("nationality").isNotNull)

    wonulldf.show()

    println("<=========nationality column with nulls==========>")

    val wnulldf = joineddf
      .filter(col("nationality").isNull)
    wnulldf.show()
    wnulldf.printSchema()

    println("<========replacing null with not available and 0=========>")

    val replaceddf = wnulldf
      .na
      .fill("Not Available")
      .na
      .fill(0)
    //val freplacedf = replaceddf.na.fill(0)
    replaceddf.show()

    println("<========adding current date to null replaced dataframes=========>")

    val replcddf = replaceddf
      .withColumn("current_date", current_date())

    replcddf.show()

    println("<========adding current date to dataframes that is not replaced=========>")

    val notreplcddf = wonulldf
      .withColumn("current_date", current_date())

    notreplcddf.show()

  }

}