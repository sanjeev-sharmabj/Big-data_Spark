package sparkproject4
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object sparkproj {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    //code to verify the default file format
    spark.read.load("file:///E:/big data/data/data.parquet").show()
    spark.read.load("file:///E:/big data/data/devices.json").show()
    spark.read.load("file:///E:/big data/data/usdata.csv").show()
    spark.read.load("file:///E:/big data/data/orcdata.orc").show()
  }

}