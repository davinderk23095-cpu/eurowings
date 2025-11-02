error id: file:///C:/Users/Davinder%20kaur/Downloads/eurowings-scala-challenge/src/main/scala/com/eurowingsholidays/ingest/BronzeIngest.scala:
file:///C:/Users/Davinder%20kaur/Downloads/eurowings-scala-challenge/src/main/scala/com/eurowingsholidays/ingest/BronzeIngest.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/functions/df0.
	 -org/apache/spark/sql/functions/df0#
	 -org/apache/spark/sql/functions/df0().
	 -df0.
	 -df0#
	 -df0().
	 -scala/Predef.df0.
	 -scala/Predef.df0#
	 -scala/Predef.df0().
offset: 944
uri: file:///C:/Users/Davinder%20kaur/Downloads/eurowings-scala-challenge/src/main/scala/com/eurowingsholidays/ingest/BronzeIngest.scala
text:
```scala
package com.eurowingsholidays.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.eurowingsholidays.ingest.utils.AppConfig
import java.time.LocalDate // for LocalDate.parse

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

    // 1) Read CSV (exactly one .csv)
  val df0@@ = spark.read
    .option("header","true")
    .option("delimiter",";")
    .option("encoding", csvEncoding)   // make sure this is UTF-8 now
    .option("multiLine","true")
    .option("mode","PERMISSIVE")
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

    // 4) Vertriebskanal-specific header fix
    val okFixed = {
      val eurSrcCol = ok.columns.find(_.contains("(Tsd. EUR)"))
        .orElse(ok.columns.find(_.toLowerCase.contains("erl"))) // fallback heuristic
      eurSrcCol match {
        case Some(src) if !ok.columns.contains("erloeswert_tsd_eur_raw") =>
          ok.withColumnRenamed(src, "erloeswert_tsd_eur_raw")
        case _ => ok
      }
    }

    // 5) Select final columns (do NOT add ingestion_date again)
    val okWithMeta = okFixed.drop(corruptCol)

    // Write bronze parquet to the dated folder
    okWithMeta.write.mode("overwrite").parquet(outRaw)

    val bronzeCount = bronzeDf.count()
    println(s"[METRICS][BRONZE] dataset=$dataset date=$ingestionDate rows=$bronzeCount path=$rawOut")

    // 6) Append errors (if any)
    if (err.head(1).nonEmpty) {
      val errWithMeta = err
        // keep types consistent (date)
        .withColumn("ingestion_date", lit(LocalDate.parse(ingestionDate)).cast("date"))
        .withColumn("input_file", input_file_name())
      errWithMeta.write.mode("append").json(outErr)
    }

    okWithMeta
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 