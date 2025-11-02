package com.eurowingsholidays.ingest

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

trait SparkTestSession extends AnyFunSuite with BeforeAndAfterAll {
  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("ew-tests")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try Option(spark).foreach(_.stop())
    finally super.afterAll()
  }
}
