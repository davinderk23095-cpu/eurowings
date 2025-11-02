package com.eurowingsholidays.ingest

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import com.eurowingsholidays.ingest.utils.AppConfig

object VertriebskanalTransform {

  def run(spark: SparkSession, cfg: AppConfig, ingestionDate: String): Unit = {
    val bronzeIn  = s"${cfg.rawPath}/Vertriebskanal/ingestion_date=$ingestionDate"
    val silverOut = s"${cfg.silverPath}/Vertriebskanal"

    // Read Bronze once
    val b = spark.read.parquet(bronzeIn)

    // Trim + snake_case all columns (same helper as Airports)
    val normalized = b.select(
      b.schema.fields.map { f =>
        val c    = col(s"`${f.name}`")
        val expr = if (f.dataType.typeName == "string") trim(c) else c
        expr.as(snake(f.name))
      }: _*
    )

    // Make sure the revenue raw column exists (robust if Bronze didn’t rename for any reason)
    val revenueCol =
      if (normalized.columns.contains("erloeswert_tsd_eur_raw")) "erloeswert_tsd_eur_raw"
      else {
        val src = normalized.columns.find(_.contains("tsd_eur"))
          .orElse(normalized.columns.find(_.contains("erloeswert")))
          .getOrElse("erloeswert_tsd_eur_raw")
        if (src == "erloeswert_tsd_eur_raw") "erloeswert_tsd_eur_raw"
        else {
          // rename dynamically
          normalized.withColumnRenamed(src, "erloeswert_tsd_eur_raw")
          "erloeswert_tsd_eur_raw"
        }
      }

    val norm2 = {
      val want = "erloeswert_tsd_eur_raw"
      if (normalized.columns.contains(want)) normalized
      else {
        val src = normalized.columns.find(_.toLowerCase.contains("tsd_eur"))
          .orElse(normalized.columns.find(_.toLowerCase.contains("erloeswert")))
          .getOrElse(want) // if not found, just assume it's already correct (no-op)
        if (src == want) normalized else normalized.withColumnRenamed(src, want)
      }
    }
    // Safe numeric parsing ("" -> NULL) for personenanzahl -> personenzahl and revenue
    val parsed = norm2
      .withColumn(
        "personenzahl",
        when(length(col("personenanzahl")) > 0,
          regexp_replace(col("personenanzahl"), ",", ".").cast("decimal(18,2)")
        ).otherwise(lit(null).cast("decimal(18,2)"))
      )
      .withColumn(
        "erloeswert_tsd_eur",
        when(length(col("erloeswert_tsd_eur_raw")) > 0,
          regexp_replace(col("erloeswert_tsd_eur_raw"), ",", ".").cast("decimal(18,3)")
        ).otherwise(lit(null).cast("decimal(18,3)"))
      )

    // Final column set (keep ingestion_date for partitioning; prefer consistent names)
    val silver = parsed.select(
      col("vertriebskanal"),
      col("produkttyp"),
      col("fluggesellschaft"),
      col("buchungsmonate").as("buchungsmonat"),
      col("reisemonate").as("reisemonat"),
      col("personenzahl"),
      col("erloeswert_tsd_eur"),
      col("input_file"),
      col("ingestion_date")
    )

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    // Metrics (use lower-case names that actually exist)
    val total    = silver.count()
    val nullKeys = silver.filter(col("vertriebskanal").isNull || col("fluggesellschaft").isNull).count()
    println(s"[METRICS][SILVER] dataset=Vertriebskanal date=$ingestionDate rows=$total null_key_cols=$nullKeys")

    // Write partitioned by ingestion_date
    silver.write
      .mode("overwrite")
      .partitionBy("ingestion_date")
      .parquet(silverOut)
  }

  // same helper as Airports
  private def snake(s: String): String =
    s.trim
      .replaceAll("[()]", "")
      .replaceAll("[ .:/-]+", "_")
      .replaceAll("ä","ae").replaceAll("ö","oe").replaceAll("ü","ue").replaceAll("ß","ss")
      .toLowerCase
}
