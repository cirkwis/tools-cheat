package com.socgen.bsc.cbs.businessviews.thirdParties.config

/**
  * DataSourceName values are used on:
  * - recognizing the DataSourceName from config files.
  * - the column names UIDs. e.g: "unique_id_from_rct" column name.
  */
object DataSourceName extends Enumeration {
  type DataSourceName = Value
  val RCT = Value("rct")
  val DUN = Value("dun")
  val AVOX_FULL = Value("avox")
  val INSEE = Value("insee")
  val LEI = Value("lei")
  val PERLE = Value("perle")
  val INFOGREFFE = Value("infogreffe")
  val IDQ = Value("idq")
  val GRC_PM = Value("grc_pm")
  val BDR = Value("bdr")
  val GRC_PP = Value("grc_pp")

  val PRIVALIA_PP = Value("privalia_pp")
  val PRIVALIA_PM = Value("privalia_pm")
  val CZ = Value("cz")
  val SK = Value("sk")
}