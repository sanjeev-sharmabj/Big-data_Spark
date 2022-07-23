package spark_june_18_19

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object complex_data_generation {

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
      .load("file:///E:/big data/data/json/jc_2.json")
    df.show()
    df.printSchema()
    
    //flattening the file
    
     println("==========flattening the file========")

    val flatdf = df.select(
      col("doorno"),
      col("orgname"),
      col("trainer"),
      col("address.permanent_city"),
      col("address.temporary_city"),
      col("street.permanent_street"),
      col("street.temporary_street"))

    flatdf.show()
    flatdf.printSchema()

    println("==========complex data generation========")
    println("==============using select statement======")
    val complexdatagen = flatdf.select(
      col("doorno"),
      col("orgname"),
      col("trainer"),
      struct(
        col("permanent_city"),
        col("temporary_city"),
        col("permanent_street"),
        col("temporary_street")).alias("address_street"))

    complexdatagen.show(20,false)
    complexdatagen.printSchema()

    complexdatagen
      .write
      .format("json")
      .mode("overwrite")
      .save("file:///E:/big data/data/json/compdatagen.json")

    println("==============using withcolumn statement======")

    val compgen = flatdf.
      withColumn(
        "address_street",
        struct(
          col("permanent_city"),
          col("temporary_city"),
          col("permanent_street"),
          col("temporary_street")))
      .drop("permanent_city")
      .drop("temporary_city")
      .drop("permanent_street")
      .drop("temporary_street")

    compgen.show(20,false)
    compgen.printSchema()

    compgen
      .write
      .format("json")
      .mode("overwrite")
      .save("file:///E:/big data/data/json/comp.json")

    println("=================done==============")

  }

}