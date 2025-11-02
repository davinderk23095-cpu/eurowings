error id: file:///C:/Users/Davinder%20kaur/Downloads/eurowings-scala-challenge/src/main/scala/com/eurowingsholidays/ingest/utils/AppConfig.scala:com/typesafe/config/Config#getString().
file:///C:/Users/Davinder%20kaur/Downloads/eurowings-scala-challenge/src/main/scala/com/eurowingsholidays/ingest/utils/AppConfig.scala
empty definition using pc, found symbol in pc: 
found definition using semanticdb; symbol com/typesafe/config/Config#getString().
empty definition using fallback
non-local guesses:

offset: 783
uri: file:///C:/Users/Davinder%20kaur/Downloads/eurowings-scala-challenge/src/main/scala/com/eurowingsholidays/ingest/utils/AppConfig.scala
text:
```scala
package com.eurowingsholidays.ingest.utils

final case class AppConfig(
  datasetName: String,
  landingPath: String,
  rawPath: String,
  errorsPath: String,
  silverPath: String,         
  overwriteSilverOnDate: Boolean,
  badRecordCol: String,
  encoding: String
)


object AppConfig {
  import com.typesafe.config.ConfigFactory

  // Nullary (no-parens) method — call it as AppConfig.load
  def load: AppConfig = {
    // Prefer conf/application.conf if present; otherwise allow env-only config
    val file = new java.io.File("conf/application.conf")
    val cfg  =
      if (file.exists) ConfigFactory.parseFile(file).resolve()
      else             ConfigFactory.load().resolve()

    AppConfig(
      landingPath  = cfg.getString("paths.landing"),
      rawPath      = cfg.@@getString("paths.raw"),
      silverPath   = cfg.getString("paths.silver"),
      badRecordCol = if (cfg.hasPath("ingest.badRecordCol")) cfg.getString("ingest.badRecordCol") else "bad_record",
      datasetName  = if (cfg.hasPath("ingest.defaultDataset")) cfg.getString("ingest.defaultDataset") else "Reiseveranstalter"
    )
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 