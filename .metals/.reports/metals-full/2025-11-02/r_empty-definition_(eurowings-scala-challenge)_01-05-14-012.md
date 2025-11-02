error id: file:///C:/Users/Davinder%20kaur/Downloads/eurowings-scala-challenge/src/main/scala/com/eurowingsholidays/ingest/BronzeIngest.scala:local13
file:///C:/Users/Davinder%20kaur/Downloads/eurowings-scala-challenge/src/main/scala/com/eurowingsholidays/ingest/BronzeIngest.scala
empty definition using pc, found symbol in pc: 
found definition using semanticdb; symbol local13
empty definition using fallback
non-local guesses:

offset: 2082
uri: file:///C:/Users/Davinder%20kaur/Downloads/eurowings-scala-challenge/src/main/scala/com/eurowingsholidays/ingest/BronzeIngest.scala
text:
```scala
package com.eurowingsholidays.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.eurowingsholidays.ingest.utils.AppConfig
import org.apache.spark.sql.functions.expr
import java.time.LocalDate   // <-- needed for LocalDate.parse

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

    // choose encoding: argument > config
    val csvEncoding = Option(encoding).filter(_.nonEmpty).getOrElse(cfg.encoding)

    // 1) Read CSV (only ONE .csv(...) call)
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
    val df1 =
      if (df0.columns.contains(corruptCol)) df0
      else df0.withColumn(corruptCol, lit(null: String))

    // 3) Split ok vs error
    val ok  = df1.filter(col(corruptCol).isNull)
    val err = df1.filter(col(corruptCol).isNotNull)

    // 4) Vertriebskanal-specific header fix (leave other datasets untouched)
    val okFixed =
      if (dataset == "Vertriebskanal") {
        val eurSrcCol = ok.columns.find(_.contains("(Tsd. EUR)")).getOrElse("Erl├Âswert (Tsd. EUR)")
        if (ok.columns.contains("erloeswert_tsd_eur_raw")) ok
        else ok.withColumnRenamed(eurSrcCol, "erloeswert_tsd_eur_raw")
      } else ok

    okWit@@hMeta.write.mode("overwrite").parquet(outRaw)

    // 6) Append errors (if any)
    if (err.head(1).nonEmpty) {
      val errWithMeta = err
        .withColumn("ingestion_date", lit(ingestionDate))
        .withColumn("input_file", input_file_name())
      errWithMeta.write.mode("append").json(outErr)
    }

    okWithMeta
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 