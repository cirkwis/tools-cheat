package com.socgen.bsc.cbs.businessviews.thirdParties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.socgen.bsc.cbs.businessviews.common.DataFrameLoader
import com.socgen.bsc.cbs.businessviews.thirdParties.models.{ThirdPartyObject, ThirdType}
import com.socgen.bsc.cbs.businessviews.utils.Driver

/**
  * The principal entry to execute the Build PP/PM component.
  */
//TODO: To refactor, and integrate this main on the global porject main class .
object Main {

  /**
    * The principal entry to execute the Build PP/PM component.
    *
    * @param args
    *             only one String argument, "buildPP" or "buildPM"
    */
  def main(args: Array[String]) {
    require(args(0) != null)

    // Instantiate spark connection
    val conf = new SparkConf()
      .setAppName(s"""#360 Build Tiers ${args(0)}""")
      .set("spark.sql.join.preferSortMergeJoin", "false")
      .setIfMissing("spark.master", "local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // can't be done, because of the Schema Avro is not serializable
    //conf.registerKryoClasses(Array(classOf[ThirdPartyObject]))

    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    import sqlContext.implicits._

    val dataFrameLoader = new DataFrameLoader(sqlContext)

    val executionType = ThirdType.withName(args(0))
    new BuildThirdsBv(dataFrameLoader, executionType).execute()
  }
}
