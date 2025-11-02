package com.eurowingsholidays.ingest

import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

class VertriebskanalGoldSpec extends SparkTestSession with Matchers {

  test("computeMoM calculates abs and pct correctly and handles first month / zero prev") {
    val schema = StructType(Seq(
      StructField("vertriebskanal",     StringType,  true),
      StructField("produkttyp",         StringType,  true),
      StructField("fluggesellschaft",   StringType,  true),
      StructField("buchungsmonat",      StringType,  true),   // "YYYYMM"
      StructField("reisemonat",         StringType,  true),
      StructField("personenzahl",       DecimalType(18,2), true),
      StructField("erloeswert_tsd_eur", DecimalType(18,3), true),
      StructField("input_file",         StringType,  true)
    ))

    val rows = Seq(
      // Slice A: prev missing → pct null
      Row("OTA","flight only","Aegean","202501","202503", BigDecimal("2.00"),  BigDecimal("10.000"), "f1"),
      Row("OTA","flight only","Aegean","202502","202503", BigDecimal("3.00"),  BigDecimal("15.000"), "f1"),
      // pct = (15-10)/10 = 0.5 ; abs = 5.000

      // Slice B: prev = 0 → pct null, abs ok
      Row("OTA","cruise","TUI Fly","202501","202504",     BigDecimal("1.00"),  BigDecimal("0.000"),  "f2"),
      Row("OTA","cruise","TUI Fly","202502","202504",     BigDecimal("1.50"),  BigDecimal("7.500"),  "f2"),

      // Different airline → separate window
      Row("OTA","flight only","Other","202501","202503",  BigDecimal("2.00"),  BigDecimal("20.000"), "f3")
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema
    )

    val out = VertriebskanalGold.computeMoM(df, runId = "test-run")

    val got = out
      .select(
        "vertriebskanal","produkttyp","fluggesellschaft","buchungsmonat","reisemonat",
        "erloeswert_tsd_eur","erloeswert_mom_abs","erloeswert_mom_pct","mom_prev_missing","data_version"
      )
      .collect()
      .toSeq

    // Utility to lookup rows by key (order-independent)
    def getRow(flug: String, buch: String): Row =
      got.find(r =>
        r.getAs[String]("fluggesellschaft") == flug &&
        r.getAs[String]("buchungsmonat") == buch
      ).getOrElse(sys.error(s"Missing row for $flug-$buch"))

    // --- Assertions (Aegean 202501) ---
    val a1 = getRow("Aegean", "202501")
    a1.getAs[java.math.BigDecimal]("erloeswert_tsd_eur").toPlainString shouldBe "10.000"
    a1.isNullAt(a1.fieldIndex("erloeswert_mom_abs")) shouldBe true
    a1.isNullAt(a1.fieldIndex("erloeswert_mom_pct")) shouldBe true
    a1.getAs[Boolean]("mom_prev_missing") shouldBe true
    a1.getAs[String]("data_version") shouldBe "test-run"

    // --- Assertions (Aegean 202502) ---
    val a2 = getRow("Aegean", "202502")
    a2.getAs[java.math.BigDecimal]("erloeswert_mom_abs").toPlainString shouldBe "5.000"
    Option(a2.getAs[java.math.BigDecimal]("erloeswert_mom_pct")).map(_.doubleValue()) shouldBe Some(0.5)
    a2.getAs[Boolean]("mom_prev_missing") shouldBe false

    // --- Assertions (TUI Fly 202501 & 202502) ---
    val t1 = getRow("TUI Fly", "202501")
    t1.isNullAt(t1.fieldIndex("erloeswert_mom_pct")) shouldBe true // prev missing for slice start

    val t2 = getRow("TUI Fly", "202502")
    t2.getAs[java.math.BigDecimal]("erloeswert_mom_abs").toPlainString shouldBe "7.500"
    t2.isNullAt(t2.fieldIndex("erloeswert_mom_pct")) shouldBe true // prev=0 => pct null

    // --- Assertions (Other airline separate slice) ---
    val o1 = getRow("Other", "202501")
    o1.isNullAt(o1.fieldIndex("erloeswert_mom_abs")) shouldBe true
  }
}