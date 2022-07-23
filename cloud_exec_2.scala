package cloud_proj

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object cloud_exec_2 {
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
      .load("file:///E:/big data/data/json/medication.json")

    val flatdf = jsondf
      .withColumn("medications", explode(col("medications")))
      .withColumn("aceInhibitors", explode(col("medications.aceInhibitors")))
      .withColumn("antianginal", explode(col("medications.antianginal")))
      .withColumn("anticoagulants", explode(col("medications.anticoagulants")))
      .withColumn("betaBlocker", explode(col("medications.betaBlocker")))
      .withColumn("diuretic", explode(col("medications.diuretic")))
      .withColumn("mineral", explode(col("medications.mineral")))
      .select(
        col("aceInhibitors.dose").alias("aceInhibitors_dose"),
        col("aceInhibitors.name").alias("aceInhibitors_name"),
        col("aceInhibitors.pillCount").alias("aceInhibitors_pillCount"),
        col("aceInhibitors.refills").alias("aceInhibitors_refills"),
        col("aceInhibitors.route").alias("aceInhibitors_route"),
        col("aceInhibitors.sig").alias("aceInhibitors_sig"),
        col("aceInhibitors.strength").alias("aceInhibitors_strength"),
        col("antianginal.dose").alias("aceInhibitors_dose"),
        col("antianginal.name").alias("antianginal_name"),
        col("antianginal.pillCount").alias("antianginal_pillCount"),
        col("antianginal.refills").alias("antianginal_refills"),
        col("antianginal.route").alias("antianginal_route"),
        col("antianginal.sig").alias("antianginal_sig"),
        col("antianginal.strength").alias("antianginal_strength"),
        col("anticoagulants.dose").alias("anticoagulants_dose"),
        col("anticoagulants.name").alias("anticoagulants_name"),
        col("anticoagulants.pillCount").alias("anticoagulants_pillCount"),
        col("anticoagulants.refills").alias("anticoagulants_refills"),
        col("anticoagulants.route").alias("anticoagulants_route"),
        col("anticoagulants.sig").alias("anticoagulants_sig"),
        col("anticoagulants.strength").alias("anticoagulants_strength"),
        col("betaBlocker.dose").alias("betaBlocker_dose"),
        col("betaBlocker.name").alias("betaBlocker_name"),
        col("betaBlocker.pillCount").alias("betaBlocker_pillCount"),
        col("betaBlocker.refills").alias("betaBlocker_refills"),
        col("betaBlocker.route").alias("betaBlocker_route"),
        col("betaBlocker.sig").alias("betaBlocker_sig"),
        col("betaBlocker.strength").alias("betaBlocker_strength"),
        col("diuretic.dose").alias("diuretic_dose"),
        col("diuretic.name").alias("diuretic_name"),
        col("diuretic.pillCount").alias("diuretic_pillCount"),
        col("diuretic.refills").alias("diuretic_refills"),
        col("diuretic.route").alias("diuretic_route"),
        col("diuretic.sig").alias("diuretic_sig"),
        col("diuretic.strength").alias("diuretic_strength"),
        col("mineral.dose").alias("mineral_dose"),
        col("mineral.name").alias("mineral_name"),
        col("mineral.pillCount").alias("mineral_pillCount"),
        col("mineral.refills").alias("mineral_refills"),
        col("mineral.route").alias("mineral_route"),
        col("mineral.sig").alias("mineral_sig"),
        col("mineral.strength").alias("mineral_strength"))
      .drop(
        "aceInhibitors",
        "antianginal",
        "anticoagulants",
        "betaBlocker",
        "diuretic",
        "mineral")
    flatdf.show()
    flatdf.printSchema()

  }
}