package com.eurowingsholidays.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.eurowingsholidays.ingest.utils.AppConfig

object VertriebskanalGold {

  // computeMoM is called from tests
  private[ingest] def computeMoM(silver: DataFrame, runId: String): DataFrame = {
    val w = Window
      .partitionBy("vertriebskanal","produkttyp","fluggesellschaft","reisemonat")
      .orderBy(col("buchungsmonat"))

    val prev = lag(col("erloeswert_tsd_eur"), 1).over(w)

    silver
      .withColumn("erloeswert_prev", prev)
      .withColumn(
        "erloeswert_mom_abs",
        when(col("erloeswert_prev").isNull, lit(null).cast("decimal(18,3)"))
          .otherwise( (col("erloeswert_tsd_eur") - col("erloeswert_prev")).cast("decimal(18,3)") )
      )
      .withColumn(
        "erloeswert_mom_pct",
        when(col("erloeswert_prev").isNull || col("erloeswert_prev") === lit(0),
             lit(null).cast("decimal(18,4)"))
          .otherwise( (col("erloeswert_tsd_eur") - col("erloeswert_prev")) / col("erloeswert_prev") )
      )
      .withColumn("mom_prev_missing", col("erloeswert_prev").isNull)
      .withColumn("data_version", lit(runId))
      .select(
        col("vertriebskanal"), col("produkttyp"), col("fluggesellschaft"),
        col("buchungsmonat"), col("reisemonat"),
        col("personenzahl"), col("erloeswert_tsd_eur"),
        col("erloeswert_mom_abs"), col("erloeswert_mom_pct"),
        col("mom_prev_missing"), col("input_file"), col("data_version")
      )
  }

  // build() is what Main calls
  def build(spark: SparkSession, cfg: AppConfig, runId: String): DataFrame = {
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    val silverPath = s"${cfg.silverPath}/Vertriebskanal"
    val goldPath   = s"${cfg.goldPath}/fact_booking_momentum"

    val silver = spark.read.parquet(silverPath)
    val gold   = computeMoM(silver, runId)

    gold
      .repartition(col("buchungsmonat"))
      .write
      .mode("overwrite") // dynamic overwrite above keeps other partitions
      .partitionBy("buchungsmonat")
      .parquet(goldPath)

    println(s"[METRICS][GOLD] rows=${gold.count()} path=$goldPath")
    gold
  }
}
