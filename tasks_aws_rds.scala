package spark_june_12

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object tasks {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val sqldf = spark
      .read
      .format("jdbc")
      .option(
        "url",
        "jdbc:mysql://database-1.cvk5bls6gwai.ap-south-1.rds.amazonaws.com/zeyodb")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "zeyotab")
      .option("user", "root")
      .option("password", "Aditya908")
      .load()
    sqldf.show()

  }

}