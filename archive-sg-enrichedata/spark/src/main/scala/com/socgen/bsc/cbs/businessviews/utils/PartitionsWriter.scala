package com.socgen.bsc.cbs.businessviews.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import com.socgen.bsc.cbs.businessviews.common.{DataFrameLoader, FsUtils}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Created by X149386 on 18/11/2016.
  */
class PartitionsWriter(dataLoader: DataFrameLoader, appName: String) {
  val env = ConfigFactory.load("environment.conf")
  val dataSets = env.getStringList("env.bvSources")
  val projectRoot = env.getString("env.projectRoot")
  val destinationPath = projectRoot + "logPartitions/" + appName + "/"
  val currentDate = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime())

  def generatePartitions(): Unit = {
    val result: ListBuffer[String] = new ListBuffer[String]
    dataSets.foreach(dataSet => {
      result.append(dataSet + "=" + dataLoader.getPartitionLocation(dataSet).getOrElse("no partition found"))
    })
    FsUtils.writeAllLines(result, new Path(destinationPath + "lastPartitions-" + currentDate + ".config"))
  }
}
