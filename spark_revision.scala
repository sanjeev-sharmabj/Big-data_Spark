package spark_revision

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.TypedColumn

object spark_revision {

  case class schema(
    txnno:    String,
    txndate:  String,
    custno:   String,
    amount:   String,
    category: String,
    product:  String,
    city:     String,
    State:    String,
    spendby:  String)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val listcol = List(
      "txnno",
      "txndate",
      "custno",
      "amount",
      "category",
      "product",
      "city",
      "State",
      "spendby")

    println("=======================slide-3======================")
    println()
    println("==========creating a list========")
    val lis = List(1, 4, 6, 7)
    lis.foreach(println)
    println("==========ADDING 2 TO THE LIST===")
    val newlist = lis.map(x => x + 2)
    newlist.foreach(println)

    println()
    println("========================slide-4======================")
    println()

    println("==========raw list=============")
    val list = List("zeyobron", "zeyo", "analytics")
    list.foreach(println)
    println("=============filtered list=========")
    val filtrlist = list
      .filter(x => x.contains("zeyo"))
    filtrlist.foreach(println)

    println("=======================slide-5======================")
    println("==========reading file-1============")
    val data = sc.
      textFile("file:///E:/big data/data/revdata/revdata/file1.txt")
    data.take(10).foreach(println)

    println("============filter the row with gymnastics====")

    val filtrrdd = data
      .filter(x => x.contains("Gymnastics"))
    filtrrdd.take(10).foreach(println)

    println("=======================slide-6======================")
    println("==========splitting the data by delimiter========")

    val splitrdd = filtrrdd.map(x => x.split(","))
    println("==========imposing case class============")

    val schrdd = splitrdd
      .map(x => schema(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
    schrdd.take(10).foreach(println)
    println("==========filtering by product data============")
    val prodfiltr = schrdd
      .filter(x => x.product.contains("Gymnastics"))
    prodfiltr.take(10).foreach(println)

    println("=======================slide-7======================")
    println("==========reading file-2============")

    val data2 = sc
      .textFile("file:///E:/big data/data/revdata/revdata/file2.txt")
    data2
      .take(10)
      .foreach(println)

    val splitdata = data2.map(x => x.split(","))
    println("==========row rdd============")
    val rowrdd = splitdata
      .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
    rowrdd.take(10)
      .foreach(println)

    println("=======================slide-8======================")
    println("=============creating a dataframe using schemardd=======")
    val df = schrdd.toDF().select(listcol.map(col): _*)
    df.show()

    println("==============creating a dataframe using rowrdd=======")

    val structsch = StructType(Array(
      StructField("txnno", StringType, true),
      StructField("txndate", StringType, true),
      StructField("custno", StringType, true),
      StructField("amount", StringType, true),
      StructField("category", StringType, true),
      StructField("product", StringType, true),
      StructField("city", StringType, true),
      StructField("State", StringType, true),
      StructField("spendby", StringType, true)))

    val rdddf = spark
      .createDataFrame(rowrdd, structsch)
      .select(listcol.map(col): _*)
    rdddf.show()

    println("=======================slide-9======================")
    println("==============reading csv file==============")
    val csvdf = spark.
      read.
      format("csv").
      option("header", "true")
      .load("file:///E:/big data/data/revdata/revdata/file3.txt")
      .select(listcol.map(col): _*)
    csvdf.show()
    csvdf.printSchema()

    println("=======================slide-10======================")
    println("==============reading json file==============")

    val jsondf = spark
      .read
      .format("json").option("multiline", "true")
      .load("file:///E:/big data/data/revdata/revdata/file4.json")
      .select(listcol.map(col): _*)
    jsondf.show()
    jsondf.printSchema()

    println("==============reading parquet file==============")

    val parquetdf = spark
      .read
      .load("file:///E:/big data/data/revdata/revdata/file5.parquet")
      .select(listcol.map(col): _*)
    parquetdf.show()
    parquetdf.printSchema()

    println("=======================slide-11======================")
    println("==============reading xml file==============")

    val xmldf = spark.
      read.
      format("xml").
      option("rowTag", "txndata")
      .load("file:///E:/big data/data/revdata/revdata/file6")
      .select(listcol.map(col): _*)
    xmldf.show()
    xmldf.printSchema()

    println("=======================slide-12======================")
    println("==============union of dataframes==============")

    val uniondf = df
      .union(rdddf)
      .union(csvdf)
      .union(jsondf)
      .union(parquetdf)
      .union(xmldf)

    uniondf.show()
    uniondf.printSchema()

    println("=======================slide-13======================")
    println("==============filters and case statement==============")

    val filtrdf = uniondf
      .withColumn("txndate", expr("split(txndate,'-')[2]"))
      .withColumnRenamed("txndate", "year")
      .withColumn(
        "status",
        expr("case when spendby = 'cash' then 1 else 0 end"))
      .filter(col("txnno") > 5000)
    filtrdf.show()

    println("=======================slide-14======================")
    println("==============group by and aggregate==============")
    val sumamount = uniondf
      .groupBy("category")
      .agg(sum(col("amount"))
        .alias("total"))
    sumamount.show()

    println("=======================slide-15======================")
    println("==============write a avro with parititions==============")

    uniondf
      .write
      .format("avro")
      .partitionBy("category")
      .mode("append")
      .save("file:///E:/big data/data/revdata/revdata/avrofile")

    println("=======================done======================")

    println("=======================slide-16======================")
    println("=======================flattening json======================")

    val jdf = spark
      .read
      .format("json")
      .option("multiline", "true")
      .load("file:///E:/big data/data/json/actors.json")

    jdf.show()
    jdf.printSchema()

    val flatjdf = jdf.
      withColumn("actors", explode(col("Actors")))
      .withColumn("childrens", explode(col("actors.children")))
      .select(
        col("actors.*"),
        col("childrens"),
        col("country"),
        col("version"))
      .withColumnRenamed("childrens", "children")
      .drop("children")
    flatjdf.show()
    flatjdf.printSchema()

  }

}