
package com.socgen.bsc.cbs.businessviews.common

import org.apache.spark.sql.{DataFrame, SaveMode}
import java.text.SimpleDateFormat
import java.util.Calendar
import com.typesafe.config.ConfigFactory
import scala.util.matching.Regex

object Frequency extends Enumeration {
  type Frequency = Value
  val DAILY, MONTHLY = Value
}

trait ViewWriter {
  def dataLoader: DataFrameLoader = null
  def frequency: Frequency.Value

  //todo: outputDirectory must be retrieved from a specific method, and through config files
  val outputDirectory = ConfigFactory.load("environment.conf").getString("env.bvLocationPath")

  def save(result: DataFrame, name: String, saveFormat: String = "parquet"): Unit = {

    val extractPartitionDate = "(\\d{4})/(\\d{2})/(\\d{2})".r
    val partitionedBy = "yyyy/mm/dd"

    val (yyyy, mm, dd) = dataLoader.partition match {
      case Some(extractPartitionDate(yyyy, mm, dd)) => (yyyy, mm, dd)
      case _ => getCurrentDate()
    }

    import org.apache.spark.sql.functions._
    val df = result.withColumn("yyyy", lit(yyyy))
      .withColumn("mm", lit(mm))
      .withColumn("dd", lit(dd))

    df.write.mode(SaveMode.Append)
      .format(saveFormat)
      .partitionBy(partitionedBy.split("/"): _*)
      .save(outputDirectory + name)
  }

  def getCurrentDate(): (String, String, String) = {
    val now = Calendar.getInstance().getTime()
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val dateString = sdf.format(now)

    val pattern = new Regex("(\\d{4})(\\d{2})(\\d{2})")
    var yyyy, dd, mm = ""

    pattern.findAllIn(dateString).matchData foreach {
      m => { yyyy = m.group(1)
        mm = m.group(2)
        dd = m.group(3)}
    }
    (yyyy, mm, dd)
  }
}

trait MultipleViews extends ViewWriter with LoggingSupport {
  def process(): Map[String, DataFrame]

  def execute(): Unit = {
    val results = process()
    results.foreach { case (name, df) => save(df, name) }
  }
}

trait MultipleViewsAndOutputFormats extends ViewWriter with LoggingSupport {
  def process(): Map[String, (DataFrame, String)]

  def execute(): Unit = {
    val results = process()
    results.foreach { case (name, (df, format)) => save(df, name, format) }
  }
}

trait BusinessView extends ViewWriter with LoggingSupport {
  def name: String

  def process(): DataFrame

  def execute(saveFormat: String = "parquet"): Unit = {
    val result = process()
    save(result, name, saveFormat)
  }

  def save(result: DataFrame): Unit = {
    save(result, name)
  }
}