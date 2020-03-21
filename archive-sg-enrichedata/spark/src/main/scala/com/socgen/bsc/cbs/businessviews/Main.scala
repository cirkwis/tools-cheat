package com.socgen.bsc.cbs.businessviews

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.socgen.bsc.cbs.businessviews.utils.{Driver, HiveSchemaCreator, PartitionsWriter, ViewBuilder}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by X150102 on 19/07/2017.
  */
object Main {


  def main(args: Array[String]) {

    val driver: Driver = new Driver()
    driver.readArguments(args)

    // Instantiate spark connection
    val conf = new SparkConf()
      .setAppName(s"""#360 Businessview for ${args(0)}""")
      .setIfMissing("spark.master", "local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    if (driver.config.appName == "buildThirdParties") {
      val configWriter: PartitionsWriter = driver.loadConfigWriter(sqlContext,"buildThirdParties")
      configWriter.generatePartitions()
    }
    else if (driver.config.appName == "bigMatching") {
      val configWriter: PartitionsWriter = driver.loadConfigWriter(sqlContext, "bigMatching")
      configWriter.generatePartitions()
    }

    if (driver.config.database.nonEmpty) {
      val hiveSchemaCreator: HiveSchemaCreator = driver.loadHiveSchemaCreator(sqlContext)
      hiveSchemaCreator.PrintSchemaCreationStatements()
    }
    else if (driver.config.views.nonEmpty) {
      val viewBuilder: ViewBuilder = driver.loadViewBuilder(sqlContext)
      viewBuilder.buildViews()
    }

    sc.stop()
  }

}
