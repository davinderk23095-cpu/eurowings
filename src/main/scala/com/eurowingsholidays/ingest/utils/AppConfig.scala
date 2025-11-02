package com.eurowingsholidays.ingest.utils

import com.typesafe.config.ConfigFactory

final case class AppConfig(
  landingPath: String,
  rawPath: String,
  silverPath: String,
  goldPath: String,           // <- add this
  datasetName: String,
  encoding: String
)

object AppConfig {
  def load: AppConfig = {
    val c = ConfigFactory.load()

    AppConfig(
      landingPath  = c.getString("landingPath"),
      rawPath      = c.getString("rawPath"),
      silverPath   = c.getString("silverPath"),
      goldPath     = if (c.hasPath("goldPath")) c.getString("goldPath")
                     else defaultGoldFromSilver(c.getString("silverPath")),
      datasetName  = c.getString("datasetName"),
      encoding     = if (c.hasPath("encoding")) c.getString("encoding") else "UTF-8"
    )
  }

  /** If goldPath is not set, derive one next to silver (…/gold). */
  private def defaultGoldFromSilver(silver: String): String = {
    // naive but effective: replace last "/silver" with "/gold"
    val idx = silver.lastIndexOf("/silver")
    if (idx >= 0) silver.substring(0, idx) + "/gold" + silver.substring(idx + "/silver".length)
    else silver + "/../gold"
  }
}
