package com.eurowingsholidays.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import com.eurowingsholidays.ingest.utils.AppConfig

object SilverTransform {

  def run(spark: SparkSession, cfg: AppConfig, ingestionDate: String): Unit = {
    val dateStr   = ingestionDate
    val datePart  = LocalDate.parse(dateStr)
    val bronzeIn  = s"${cfg.rawPath}/Reiseveranstalter/ingestion_date=$dateStr"
    val silverOut = s"${cfg.silverPath}/Reiseveranstalter"    

    val b = spark.read.parquet(bronzeIn)

    // Find the revenue column even if encoding is weird (e.g., "Erl├â┬Âswert (Tsd. EUR)")
    // replace your revenueCol detection with:
    def norm(s: String) = s.toLowerCase.replaceAll("[^a-z0-9]", "")
    val revFromMarker = b.columns.find(_.contains("(Tsd. EUR)"))
    val revFromHeur   = b.columns.find(c => {
      val n = norm(c)
      n.contains("erloes") || n.contains("erlos") || n.contains("erl") || n.contains("umsatz")
    })
    val revenueCol = (revFromMarker orElse revFromHeur).getOrElse(throw new IllegalArgumentException("Revenue column not found in Reiseveranstalter"))

    val cleaned =
      b.withColumnRenamed("Reiseveranstalter (Konzern-Ebene)", "rv_konzern")
       .withColumnRenamed("Reiseveranstalter (Marken-Ebene)" , "rv_marke")
       .withColumnRenamed("Buchungsmonate"                    , "buchungsmonat")
       .withColumnRenamed("Reisemonate"                        , "reisemonat")
       // fix decimal commas and cast to numeric
       .withColumn("personenzahl",
          regexp_replace(col("Personenanzahl"), ",", ".").cast("decimal(18,2)"))
       .drop("Personenanzahl", revenueCol)
       .withColumn("ingestion_date", lit(datePart).cast("date"))
       .select(
         col("rv_konzern"), col("rv_marke"),
         col("buchungsmonat"), col("reisemonat"),
         col("input_file"),
         col("personenzahl"),
         col("erloeswert_tsd_eur"),
         col("ingestion_date")
       )

    cleaned.write
      .mode("overwrite")
      .partitionBy("ingestion_date")
      .parquet(silverOut)
  }
}
