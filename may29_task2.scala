package sparkproject4
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object may29_task2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._
    println("========reading avro file========")

    val readavro = spark.
      read.
      format("avro").
      load("file:///E:/big data/data/data.avro")
    readavro.show()
    readavro.createOrReplaceTempView("rddtodf")

    val tordd = readavro.
      map(x => x.toString()).rdd
    tordd.foreach(println)

    /*val tordd = readavro.map(x=> x.toString()).rdd
    tordd.foreach(println)*/

    /*val tordd = readavro.rdd.map(x=> x mkString(","))
    tordd.foreach(println)
					 */

  }

}