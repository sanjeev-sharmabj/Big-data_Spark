package pac
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object dataframeswithSQL {

  case class schema(txno: String, txndate: String, category: String, product: String)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val simpleSchema = StructType(Array(
      StructField("txnno", StringType, true),
      StructField("txndate", StringType, true),
      StructField("category", StringType, true),
      StructField("product", StringType, true)))

    val data = sc.textFile("file:///E:/big data/data/datat.txt")
    data.foreach(println)

    val mapsplit = data.map(x => x.split(","))

    println("===========SchemaRdd==============")

    val schemardd = mapsplit.map(x => schema(x(0), x(1), x(2), x(3)))
    val schemadf = schemardd.toDF()
    schemadf.show()
    schemadf.createOrReplaceTempView("txndf")
    spark.sql("Select * from txndf where product like '%Gymnastics%'").show()

    println("==============RowRDD===============")
    val rowrdd = mapsplit.map(x => Row(x(0), x(1), x(2), x(3)))
    val df = spark.createDataFrame(rowrdd, simpleSchema)
    df.show()
    df.createOrReplaceTempView("rowdf")
    spark.sql("Select * from rowdf where product like '%Gymnastics%'").show()

  }
}