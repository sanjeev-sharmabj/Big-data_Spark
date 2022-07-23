package spark_june_05

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object topic_DSL_tasks {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.
      read.
      format("csv").
      option("header", "true").
      load("file:///E:/big data/data/allcountry_updated.csv")
    df.persist()
    df.show()

    println("|===========task-1=============|")

    val timstmp = df.select(
      col("id"),
      expr("year(from_unixtime(unix_timestamp(tdate,'dd-MM-yyyy')))as year"),
      expr("month(from_unixtime(unix_timestamp(tdate,'dd-MM-yyyy')))as month"),
      expr("day (from_unixtime(unix_timestamp(tdate,'dd-MM-yyyy')))as day"),
      col("name"),
      col("check"),
      col("spendby"),
      col("country"))
    timstmp.show()

    println("|==========task-2--> case=================|")

    val repldata = df.
      selectExpr(
        "id",
        "tdate",
        "name",
        """case 
        when check='I' then 'INDIA' 
        when check='K' then 'UnitedK' 
        when check = 'S' then 'United States' end as check""",
        "spendby", "country")
    repldata.show()

    println("|==========task-3--> sorting=================|")

    val procdata = df.selectExpr(
      "id",
      "split(tdate,'-')[2] as year",
      "initcap(name) as name",
      "check",
      "spendby",
      "UPPER(country)",
      "case when spendby='cash' then 0 else 1 end as status").orderBy(col("name").desc)
    procdata.show()

    println("================done=================")
  }
}