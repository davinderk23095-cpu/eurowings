package com.eurowingsholidays.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.eurowingsholidays.ingest.utils.AppConfig

object AirportsTransform {

  def run(spark: SparkSession, cfg: AppConfig, ingestionDate: String): Unit = {
    val dateStr   = ingestionDate
    val bronzeIn  = s"${cfg.rawPath}/Airports/ingestion_date=$dateStr"
    val silverOut = s"${cfg.silverPath}/Airports"

    val b = spark.read.parquet(bronzeIn)

    val normalized = b.select(
      b.schema.fields.map { f =>
        val c = col(s"`${f.name}`")
        val expr = if (f.dataType.typeName == "string") trim(c) else c
        expr.as(snake(f.name))
      }: _*
    )

    val parsed = normalized
      .withColumn(
        "personenanzahl_num",
        when(length(col("personenanzahl")) > 0,
          regexp_replace(col("personenanzahl"), ",", ".").cast("double")
        ).otherwise(lit(null).cast("double"))
      )
      .withColumn(
        "erloeswert_tsd_eur",
        when(length(col("erloeswert_tsd_eur_raw")) > 0,
          regexp_replace(col("erloeswert_tsd_eur_raw"), ",", ".").cast("double")
        ).otherwise(lit(null).cast("double"))
      )

    val guarded = parsed
      .withColumn("abflughafen",
        when(trim(col("abflughafen")) === "" || col("abflughafen").isNull, lit(null)).otherwise(col("abflughafen"))
      )
      .withColumn("zielflughafen",
        when(trim(col("zielflughafen")) === "" || col("zielflughafen").isNull, lit(null)).otherwise(col("zielflughafen"))
      )

    val silver = guarded.select(
      col("reiseveranstalter_x_trad_"),
      col("abflughafen"),
      col("zielflughafen"),
      col("buchungsmonate"),
      col("reisemonate"),
      col("input_file"),
      col("personenanzahl_num"),
      col("erloeswert_tsd_eur"),
      col("ingestion_date") // keep for partitioning
    )

    val total      = silver.count()
    val nullKeys   = silver.filter(col("abflughafen").isNull || col("zielflughafen").isNull).count()
    val badPers = normalized.filter(
      col("personenanzahl").isNull ||
      length(trim(col("personenanzahl"))) === 0 ||
      !regexp_replace(col("personenanzahl"), ",", ".").rlike("^[-+]?[0-9]*\\.?[0-9]+$")
    ).count()

    val badRevenue = normalized.filter(
      col("erloeswert_tsd_eur_raw").isNull ||
      length(trim(col("erloeswert_tsd_eur_raw"))) === 0 ||
      !regexp_replace(col("erloeswert_tsd_eur_raw"), ",", ".").rlike("^[-+]?[0-9]*\\.?[0-9]+$")
    ).count()

    println(s"[METRICS][SILVER] dataset=Airports date=$ingestionDate rows=$total null_key_cols=$nullKeys bad_personen=$badPers bad_revenue=$badRevenue")

    silver.write
      .mode("overwrite")
      .partitionBy("ingestion_date")
      .parquet(silverOut)
  }

  private def snake(s: String): String =
    s.trim
      .replaceAll("[()]", "")
      .replaceAll("[ .:/-]+", "_")
      .replaceAll("ä","ae").replaceAll("ö","oe").replaceAll("ü","ue").replaceAll("ß","ss")
      .toLowerCase
}
