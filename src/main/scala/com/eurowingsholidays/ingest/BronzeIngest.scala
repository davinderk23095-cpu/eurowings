package com.eurowingsholidays.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.eurowingsholidays.ingest.utils.AppConfig
import java.time.LocalDate

object BronzeIngest {

  def copyToBronze(
      spark: SparkSession,
      cfg: AppConfig,
      dataset: String,
      ingestionDate: String,
      encoding: String
  ): DataFrame = {

    val fileDate   = ingestionDate.replace("-", "")
    val inputPath  = s"${cfg.landingPath}/${dataset}_${fileDate}.csv"
    val outRaw     = s"${cfg.rawPath}/${dataset}/ingestion_date=${ingestionDate}"
    val outErr     = s"${cfg.rawPath}/${dataset}/_errors/ingestion_date=${ingestionDate}"
    val corruptCol = "_corrupt_record"

    // prefer CLI arg over config
    val csvEncoding = Option(encoding).filter(_.nonEmpty).orElse(Option(cfg.encoding)).getOrElse("UTF-8")

    // 1) Read CSV
    val df0 = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("encoding", csvEncoding)
      .option("multiLine", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", corruptCol)
      .csv(inputPath)
      .withColumn("ingestion_date", lit(LocalDate.parse(ingestionDate)).cast("date"))
      .withColumn("input_file", input_file_name())

    // 2) Ensure corrupt column exists
    val bronzeDf =
      if (df0.columns.contains(corruptCol)) df0
      else df0.withColumn(corruptCol, lit(null: String))

    // 3) Split ok vs error
    val ok  = bronzeDf.filter(col(corruptCol).isNull)
    val err = bronzeDf.filter(col(corruptCol).isNotNull)

    // 4) Optional header fix (keeps your heuristic)
    val okFixed = {
      val eurSrcCol = ok.columns.find(_.contains("(Tsd. EUR)"))
        .orElse(ok.columns.find(_.toLowerCase.contains("erl"))) // fallback heuristic
      eurSrcCol match {
        case Some(src) if !ok.columns.contains("erloeswert_tsd_eur_raw") =>
          ok.withColumnRenamed(src, "erloeswert_tsd_eur_raw")
        case _ => ok
      }
    }

    // 5) Final bronze selection (drop corrupt marker)
    val okWithMeta = okFixed.drop(corruptCol)

    // Write bronze parquet (folder already includes the date)
    okWithMeta.write.mode("overwrite").parquet(outRaw)

    // Metrics
    val okCount  = okWithMeta.count()
    val errCount = err.count()
    println(s"[METRICS][BRONZE] dataset=$dataset date=$ingestionDate ok_rows=$okCount err_rows=$errCount path=$outRaw")

    // 6) Append errors (if any)
    if (errCount > 0) {
      val errWithMeta = err
        .withColumn("ingestion_date", lit(LocalDate.parse(ingestionDate)).cast("date"))
        .withColumn("input_file", input_file_name())
      errWithMeta.write.mode("append").json(outErr)
    }

    okWithMeta
  }
}
