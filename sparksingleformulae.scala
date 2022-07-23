package pac

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object sparksingleformulae {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    //below is the code to read the data

    println("====================csv=========")

    val csvdf = spark.
      read.
      format("csv").
      option("header", "true").load("file:///E:/big data/data/usdata.csv")
    //csvdf.show(10)
    csvdf.createOrReplaceTempView("csvtab")
    spark.sql("Select * from csvtab limit 5").show()

    println("====================json=========")

    val jsondf = spark.
      read.
      format("json").
      load("file:///E:/big data/data/devices.json")
    jsondf.show(10)

    println("====================parquet=========")

    val parquetdf = spark.
      read.
      format("parquet").
      load("file:///E:/big data/data/data.parquet")
    parquetdf.show(10)

    println("====================orc=========")

    val orcdf = spark.
      read.
      format("orc").
      load("file:///E:/big data/data/orcdata.orc")
    orcdf.show(10)

    println("======avro--will not work by default but should work upon adding external jars for avro======")
    val avrodf = spark.
      read.
      format("avro").
      load("file:///E:/big data/data/data.avro")
    avrodf.show()

    //below is the code to write the data

    //we need to create a data frame to write data into different file formats"

    csvdf.createOrReplaceTempView("csvwrite")
    //creating a dataframe
    val filtrdf = spark.sql("select * from csvwrite where age>30")
    filtrdf.show()

    println("==parquet file format====")
    filtrdf.
      write.
      format("parquet").
      mode("overwrite").
      save("file:///E:/big data/data/writingdataframes/parquetdata")

    println("==json file format====")
    filtrdf.
      write.
      format("json").
      mode("overwrite").save("file:///E:/big data/data/writingdataframes/jsondata")

    println("==avro file format====")
    filtrdf
      .write
      .format("avro")
      .mode("overwrite")
      .save("file:///E:/big data/data/writingdataframes/avrodata")

    println("==orc file format====")

    filtrdf
      .write
      .format("orc")
      .mode("overwrite")
      .save("file:///E:/big data/data/writingdataframes/orcdata")

    //checking the modes--execute one at a time

    filtrdf.
      write.format("csv").
      save("file:///E:/big data/data/writingdataframes/csvdata")

    filtrdf.
      write.
      format("csv").
      mode("append").
      save("file:///E:/big data/data/writingdataframes/csvdata")

    filtrdf.
      write.
      format("csv").
      mode("error").
      save("file:///E:/big data/data/writingdataframes/csvdata")

    filtrdf.
      write.
      format("csv").
      mode("overwrite").
      save("file:///E:/big data/data/writingdataframes/csvdata")

    filtrdf.
      write.
      format("csv").
      mode("ignore").
      save("file:///E:/big data/data/writingdataframes/csvdata")

    println("===============done==================")

  }

}