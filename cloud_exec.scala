package cloud_proj

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object cloud_exec {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("cloud")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val jsondf = spark
      .read
      .format("json")
      .option("multiline", "true")
      .load("file:///E:/big data/data/json/reqres.json")

    val flatdf = jsondf
      .withColumn("data", explode(col("data")))
      .select(
        col("data.*"),
        col("page"),
        col("per_page"),
        col("support.*"),
        col("total"),
        col("total_pages"))

    flatdf.show()
    flatdf.printSchema()

  }
}