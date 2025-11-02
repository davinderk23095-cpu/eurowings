package com.eurowingsholidays.ingest

import org.apache.spark.sql.SparkSession
import com.eurowingsholidays.ingest.utils.AppConfig

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EW Scala Challenge - Task 1")
      .master("local[*]")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    val cfg = AppConfig.load

    val params = args.sliding(2, 2).collect {
      case Array("--dataset", v)       => "dataset"       -> v
      case Array("--ingestionDate", v) => "ingestionDate" -> v
      case Array("--encoding", v)      => "encoding"      -> v
    }.toMap

    val dataset       = params.getOrElse("dataset", cfg.datasetName)
    val ingestionDate = params.getOrElse("ingestionDate", java.time.LocalDate.now.toString)
    val encoding      = params.getOrElse("encoding", "UTF-8")

    // 1) landing -> bronze
    BronzeIngest.copyToBronze(spark, cfg, dataset, ingestionDate, encoding)

    // 2) bronze -> silver
    dataset match {
      case "Reiseveranstalter" => SilverTransform.run(spark, cfg, ingestionDate)
      case "Vertriebskanal"    => VertriebskanalTransform.run(spark, cfg, ingestionDate)
      case "Airports"          => AirportsTransform.run(spark, cfg, ingestionDate)
      case other               => println(s"[WARN] No silver transform defined for dataset=$other")
    }

    // 3) silver -> gold (only when Silver is ready)
    if (dataset == "Vertriebskanal") {
      VertriebskanalGold.build(spark, cfg, runId = ingestionDate)
    }

    println(s"[RUN OK] dataset=$dataset date=$ingestionDate")
    spark.stop()
  }
}
