package com.socgen.bsc.cbs.businessviews.common

import java.io.FileNotFoundException
import java.net.URLDecoder

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.socgen.bsc.cbs.businessviews.thirdParties.GlobalMatchedPPBv

import scala.util.{Failure, Success, Try}

class DataFrameLoader(var sqlContext: SQLContext, var partition: Option[String] = None,
                      var useExternalFileConfig: Boolean = false, var isBvEngineProcess: Boolean = false) {

  val bvPathLocation = ConfigFactory.load("environment.conf").getString("env.bvLocationPath")
  var dataSetsPartitions: Map[String, Path] = Map()

  private def findPartitionsFromFileConfig(): Map[String, Path] = {
    Map()
  }

  def formatPartition(partition: Option[String]) : Option[String] = {
    val partitionReg = "(\\d{4})/(\\d{2})/(\\d{2})".r
    partition match {
      case Some(partitionReg(yyyy, mm, dd)) => Some(s"yyyy=${yyyy}/mm=${mm}/dd=${dd}")
      case _ => None
    }
  }

  def getDataDirectory(dataSetName: String): Path = {
    if (dataSetName.endsWith("_bv") || dataSetName.endsWith("bigdata")) {
      new Path(bvPathLocation + dataSetName)
    } else if (isBvEngineProcess) {
      //Dirty ack for manage bv engine
      new Path(bvPathLocation + dataSetName + "/" + formatPartition(partition).getOrElse(""))
    } else {
      val config = ConfigFactory.load("ref_" + dataSetName)
      val dataDirectory = config.getString("sg.cbs.enrichedata." + dataSetName + ".output.dataDirectory") + "/" + formatPartition(partition).getOrElse("")
      new Path(dataDirectory)
    }
  }

  def findLatestPartition(dataSetName: String): Option[Path] = {
    val dataDirectory = getDataDirectory(dataSetName)
    if (FsUtils.exists(dataDirectory)) {
      val directories = FsUtils.listDirs(dataDirectory)
      directories.toList.map(_.toString).sorted.lastOption.map(new Path(_))
    } else None
  }

  def getPartitionLocation(dataSetName: String): Option[Path] = {
    useExternalFileConfig match {
      case false => findLatestPartition(dataSetName)
      case true => dataSetsPartitions.get(dataSetName)
    }
  }

  def loadWithOption(dataSetName: String): Option[DataFrame] = {

    val partitionLocation: Option[Path] = getPartitionLocation(dataSetName)
    println("debug: " + dataSetName + ": " + partitionLocation)

    val resultDF = partitionLocation match {
      case Some(path) => {
        if (dataSetName.equals("global_matched_bv") || dataSetName.equals("matching_pp_bv"))
          Some(sqlContext.read.json(partitionLocation.get.toString))
        else
          Try(sqlContext.read.parquet(URLDecoder.decode(partitionLocation.get.toString, "UTF-8"))) match {
            case Success(df) => Some(df)
            case Failure(e) => None
          }
      }
      case None => None //throw new FileNotFoundException(s"Error: no partition found for the dataSet $dataSetName")
    }

    if (resultDF.isDefined)
      println("debug: partitions number for [" + dataSetName + "] = " + resultDF.get.rdd.getNumPartitions)

    resultDF
  }

  def load(name: String): DataFrame =
    loadWithOption(name) match {
      case Some(df) => df
      case None => throw new FileNotFoundException(s"Error: no partition found for the dataSet $name")
    }

  def loadAll(dataSetName: String): Option[DataFrame] = {
    Try(sqlContext.read.parquet(bvPathLocation + "/" + dataSetName)) match {
      case Success(df) => Some(df)
      case Failure(e) => None
    }
  }
}
