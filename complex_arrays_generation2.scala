package spark_june_25_26

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object complex_arrays_generation2 {

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
      .load("file:///E:/big data/data/json/jc5.json")
    df.show(20, false)
    df.printSchema()
    
    println("===========flattening the file==========")

    val flattendf = df.
      withColumn("Students", explode(col("Students")))
      .select(
        col("Students.user.*"),
        col("doorno"),
        col("orgname"),
        col("trainer"))

    flattendf.show()
    flattendf.printSchema()
    
    println("===============reverting the array==========")

    val revertarray = flattendf
      .groupBy(
        "doorno",
        "orgname",
        "trainer")
      .agg(collect_list(struct(
        struct(
          col("location"),
          col("name"))
          .alias("user")))
        .alias("Students"))
    revertarray.show()
    revertarray.printSchema()

  }

}