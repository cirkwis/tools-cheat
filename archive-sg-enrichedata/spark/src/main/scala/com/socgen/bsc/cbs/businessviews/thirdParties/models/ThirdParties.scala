package com.socgen.bsc.cbs.businessviews.thirdParties.models

import java.util.UUID

import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import com.socgen.bsc.cbs.businessviews.common.{DataFrameLoader, LoggingSupport}
import com.socgen.bsc.cbs.businessviews.thirdParties.avro.AvroFunctions
import com.socgen.bsc.cbs.businessviews.thirdParties.config.{DataSourceName, ThirdPartiesBvConfig}
import DataSourceName._
import ThirdPartiesBvConfig._
import com.socgen.bsc.cbs.businessviews.thirdParties.models.ThirdType._
import com.socgen.bsc.cbs.businessviews.thirdParties.tools.DataFrameLoaderWithPrefix
import org.apache.spark.sql.functions._

/**
  * Created by X153279 on 17/01/2017.
  */

object ThirdParties extends LoggingSupport {
  val swidWithDistributedNulls = "swid_with_distributed_nulls"

  def skewDF(df: DataFrame, byColName: String): DataFrame = {
    import org.apache.spark.sql.functions.{concat, lit, round, rand}
    val numNullValues = 1000000
    // Just use a number that will always be bigger than number of partitions
    val swidWithDistributedNullsCol = when(df.col(byColName).isNull, concat(lit("NULL_SWID_"), round(rand.multiply(numNullValues)))).otherwise(df.col(byColName))
    df.withColumn(swidWithDistributedNulls, swidWithDistributedNullsCol)
  }

  def enrichByUIDs(dfToEnrich: DataFrame,
                   usedDataSources: List[DataSourceName],
                   thirdPartyIDs: Option[DataFrame]): Option[DataFrame] = {
    require(!usedDataSources.isEmpty)
    thirdPartyIDs match {
      case Some(data) => {
        val enrichedDF = usedDataSources.foldLeft(dfToEnrich) {
          (df, ds) => {
            debug("enrichByUIDs for: " + ds)
            val dataSourceThirdPartyIDs = ThirdParties.getIDsFor(ds, thirdPartyIDs.get)

            val skewedDF = skewDF(df, bigMatchingColMappingName(ds))

            skewedDF.join(dataSourceThirdPartyIDs, skewedDF(bigMatchingColMappingName(ds)) === dataSourceThirdPartyIDs("source_id"), "left_outer")
              .drop("source_id")
              .drop(swidWithDistributedNulls)
          }
        }
        Some(enrichedDF)
      }
      case None => {
        debug("debug: The Third Party IDs mapping table is empty!")
        None
      }
    }
  }

  /**
    * Enrich the matched Third Parties by the data Sources
    * specified on "usedDataSources" variable.
    */
  def enrichByDataSources(avroSchemaPath: String,
                          avroSchemaFiles: List[String],
                          usedDataSources: List[DataSourceName],
                          thirdType: ThirdType = PM,
                          dfToEnrich: DataFrame,
                          dataLoader: DataFrameLoader): DataFrame = {
    import org.apache.spark.sql.functions._

    val avroSchema: Schema = AvroFunctions.getAvroSchema(avroSchemaPath, (avroSchemaFiles): _*)

    require(!usedDataSources.isEmpty)

    val enrichedDF = usedDataSources.foldLeft(dfToEnrich) {
      (df, ds) => {
        import scala.collection.JavaConversions._
        //TODO: to be refactored "usedColumns" field
        val usedColumns: Set[String] = avroSchema.getFields.map {
          avroField => {
            val fromNodes = avroField.getJsonProp("from")
            if (fromNodes == null)
              null
            else {
              var returnedFieldName: String = null
              for (element <- fromNodes.getElements) {
                val dataSourceName = DataSourceName.withName(element.get("dataSource").getTextValue)
                val fieldName = DataFrameLoaderWithPrefix.colPrefixed(dataSourceName, element.get("fieldName").getTextValue)

                if (ds.equals(dataSourceName))
                  returnedFieldName = fieldName
              }
              returnedFieldName
            }
          }
        }.filter(_ != null).toSet + DataFrameLoaderWithPrefix.colPrefixed(ds, keyForJoin(ds))

        val dataSourceKey = keyForJoin(ds)
        val bigMatchingMappedkey = bigMatchingColMappingName(ds)

        val dataSourceBvDf = DataFrameLoaderWithPrefix.loadWithPrefix(dataLoader, ds)

        //debug("Missing columns from BV " + ds + ": ")
        //usedColumns.filterNot(dataSourceBvDf.schema.fieldNames.contains(_)).foreach(debug)

        val usedColumnsDf = usedColumns.intersect(dataSourceBvDf.schema.fieldNames.toSet)

        val dfSkewed = skewDF(df, bigMatchingMappedkey)

        dfSkewed.join(dataSourceBvDf.select(usedColumnsDf.toSeq.map(col): _*), dfSkewed(swidWithDistributedNulls) === dataSourceBvDf(DataFrameLoaderWithPrefix.colPrefixed(ds, dataSourceKey)), "left_outer")
          .drop(swidWithDistributedNulls)
      }
    }

    thirdType match {
      case PM => enrichedDF
      case _ => enrichedDF
    }
  }

  /**
    * Format each line on "enrichedDF" to a ContainerResult
    *
    * @return
    * RDD[ContainerResult]
    */
  // Treat the broken list inside
  def compute(avroSchemaPath: String,
              avroSchemaFiles: List[String],
              usedDataSources: List[DataSourceName],
              enrichedDF: DataFrame): RDD[ContainerResult] = {

    val result = enrichedDF.mapPartitions {
      //TODO: The partitions are not equitables, to be reviewed.
      (it: Iterator[Row]) => {
        val avroSchema: Schema = AvroFunctions.getAvroSchema(avroSchemaPath, (avroSchemaFiles): _*)

        it.map {
          (row: Row) => {
            val thirdPartyObject = ThirdPartyObject(row, usedDataSources, avroSchema)
            thirdPartyObject.prepare()
          }
        }
      }
    }

    processBrokenLinks(result)
  }

  //def breakLinks(line: (String, Iterable[ContainerResult])): Iterable[ContainerResult] = {
  def processBrokenLinks(containers: RDD[ContainerResult]): RDD[ContainerResult] = {
    containers.map(tp => (tp.uuid, tp))
      .groupByKey()
      .flatMap {
        case (str, itr) => {
          // If the size a broken Third Parties that have the same UUID == 1, don't do anything,
          // otherwise, process the broken links.
          if (itr.size == 1)
            itr
          else
            itr.map {
              (container: ContainerResult) => {
                if (container.uuids.get(RCT) != None) {
                  container
                } else {
                  // Generate new UUID, updated the Row and add it on tpIDs.
                  val newUUID: String = UUID.randomUUID.toString
                  val updatedRow = Row.fromSeq(Seq(newUUID) ++ container.row.toSeq.tail)

                  val mappingIDs = container.uuids.map {
                    case (dataSourceName, uid) => {
                      val sourceID = container.sourceIDs.get(dataSourceName).get

                      //println(s"Broken link is detected for the third party source_id='$sourceID' from data_source='$dataSourceName'")
                      ThirdPartyID(usedNameForOutput(dataSourceName), sourceID, newUUID)
                    }
                  }

                  ContainerResult(row = updatedRow, thirdPartyIDs = mappingIDs, uuid = newUUID)
                }
              }
            }
        }
      }

  }

  /**
    * Get all the available IDs for a specific Data Source Name.
    *
    * @param ds
    * [[DataSourceName]]
    * @return
    * [[DataFrame]] with 2 columns: source_id, unique_id_from_${DataSourceName}
    */
  def getIDsFor(ds: DataSourceName,
                thirdPartyIDsDF: DataFrame): DataFrame = {
    val sqlContext = thirdPartyIDsDF.sqlContext
    import sqlContext.implicits._

    //GroupBy Date, and get the fresh one
    thirdPartyIDsDF.filter("data_source like '" + usedNameForOutput(ds) + "'")
      .rdd
      .map(row => (row.getAs[String]("source_id"), row))
      .reduceByKey(reduceByRecentDate).map{
      case (_, r:Row) => {
        ThirdPartyID(null, r.getAs[String]("source_id"), r.getAs[String]("unique_id"))
      }
    }.toDF()
      .drop("data_source")
      .withColumnRenamed("unique_id", "unique_id_from_" + ds)
  }

  val reduceByRecentDate = (row1: Row, row2: Row) => {
    def toDate(ymd: String) = {
      val ymdFormat = org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd")
      try{ Some(ymdFormat.parseDateTime(ymd)) }
      catch { case e:Exception => None }
    }

    val date1: String = row1.get(row1.fieldIndex("yyyy")).toString + "-" + row1.get(row1.fieldIndex("mm")).toString + "-" + row1.get(row1.fieldIndex("dd")).toString
    val date2: String = row2.get(row2.fieldIndex("yyyy")).toString + "-" + row2.get(row2.fieldIndex("mm")).toString + "-" + row2.get(row2.fieldIndex("dd")).toString

    if (toDate(date1).get.isBefore(toDate(date2).get)) row2 else row1
  }
}