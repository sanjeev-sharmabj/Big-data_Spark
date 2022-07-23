package sparkproject4

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object may_29_task3 {

  case class schema(id: String, name: String, comp_name: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val csvread = spark.read.format("csv").option("header", "true").
    option("quote","\"").option("escape","\"").load("file:///E:/big data/data/rd.csv")
    csvread.show()

/*    val data = sc.textFile("file:///E:/big data/data/rd.csv")
    //data.foreach(println)
    val data1 = data.filter(x => x.contains("\""))
    val data2 = data.filter(x => !(x.contains("\"")))
    val datamap1 = data1.map(x => x.split(",")).map(x => schema(x(0), x(1) + ",", x(2)))
    val datamap2 = data2.map(x => x.split(",")).map(x => schema(x(0), x(1), x(2)))
    val finalrdd = datamap1.union(datamap2)
    finalrdd.foreach(println)
*/  }

}