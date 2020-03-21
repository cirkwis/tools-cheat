package com.socgen.bsc.cbs.businessviews.thirdParties.models

import java.util.UUID

import org.apache.avro.Schema
import org.apache.spark.sql.Row
import com.socgen.bsc.cbs.businessviews.thirdParties.config.{DataSourceName, ThirdPartiesBvConfig}
import DataSourceName._
import com.socgen.bsc.cbs.businessviews.thirdParties.models.ThirdPartyObject._
import ThirdPartiesBvConfig._

import scala.collection.JavaConversions._

/**
  * Created by X153279 on 17/01/2017.
  */

//TODO: to find an other alternative to avoid underscoreCase named fields.
case class ThirdPartyID(data_source: String, source_id: String, unique_id: String)
case class ContainerResult(row: Row, thirdPartyIDs: Iterable[ThirdPartyID], uuids: Map[DataSourceName, Option[String]] = null, sourceIDs: Map[DataSourceName, String] = null, uuid: String)

object ThirdPartyObject {
  val fieldsToDropFromHashing = Seq("unique_id", "id", "version")

  def apply(row: Row, usedDataSources: List[DataSourceName], avroSchema: Schema): ThirdPartyObject =
    new ThirdPartyObject(row, usedDataSources, avroSchema)
}

/**
  * The ThirdPartyObject represents a unique third party. This abstraction makes things simple
  * to prepare, format, all the needed fields and represent it as a Row object. The Row object contains
  * a set of fields, as specified on the Avro Schema.
  * The Row object will be transformed at the end to Avro format, and persisted on HDFS.
  *
  * @param row
  * Contains all the uniqueIDs fields per DataSource, and the needed business fields per DataSource.
  * @param usedDataSources
  * The global DataSources used to construct the Third Parties Business View.
  * @param avroSchema
  * The Avro Schema which represents a intended Third Party Object.
  */
class ThirdPartyObject(row: Row,
                       usedDataSources: List[DataSourceName],
                       avroSchema: Schema) extends Serializable {

  // Fields contain the whole Avro Fields, and their calculated values.
  var fields: Map[String, Any] = Map.empty[String, Any]

  // Technical fields.
  var UID: String = null
  var hash: Int = -1

  /**
    * "matchedDataSources" defines the matched data sources for this Third Party.
    */
  @transient lazy val matchedDataSources: Seq[DataSourceName] = {
    usedDataSources.filter {
      (ds) => {
        row.getAs[String](bigMatchingColMappingName(ds)) != null
      }
    }
  }

  /**
    * Retrieve the UID for each "marchedDataSources"
    */
  @transient lazy val UIDs: Map[DataSourceName, Option[String]] = {
    matchedDataSources
      .map {
        (ds) => {
          val uniqueIdFromColName = "unique_id_from_" + ds

          // Verify if the uniqueID is retrieved for a DataSource is necessary for the
          // the first Build Tiers execution, where there is no unique IDs mapping table available.
          ThirdPartyField.isExistOnRowSchema(uniqueIdFromColName, row.schema) match {
            case true => {
              val value = row.getAs[String](uniqueIdFromColName) match {
                case null => None
                case v => Some(v)
              }
              (ds, value)
            }
            case false => (ds, None)
          }
        }
      }.toMap[DataSourceName, Option[String]]
  }

  /**
    * Retrieve the source ID for each "matchedDataSources"
    */
  @transient lazy val sourceIDs: Map[DataSourceName, String] = {
    matchedDataSources.map {
      case (ds: DataSourceName) => {
        val bigMatchingColName = bigMatchingColMappingName(ds)
        (ds, row.getAs[String](bigMatchingColName))
      }
    }.toMap[DataSourceName, String]
  }

  /**
    * Calculate UID for the current Third Party.
    *
    * @return
    * [[String]]
    */
  def calculateUID(): String = {
    //TODO: TP: Take the UUID for RCT, If it is available, if not take the first one.
    //DONE: To Be Tested
    val map = UIDs.filter(_._2 != None)

    if (map.isEmpty) {
      return UUID.randomUUID.toString
    }

    map.get(RCT) match {
      case Some(x) => x.get
      case _ => map.head._2.get
    }
  }

  /**
    * Calculate the Hash value for the current Third Party.
    * The fields taken into account are those listed on the Avro Schema - fieldsToDropFromHashing.
    */
  def calculateHash(): Int = {
    val orderedFieldsToBeHashed = avroSchema.getFields
      .map {
        (avroField) => {
          (fieldsToDropFromHashing.contains(avroField.name())) match {
            case true => "toNotBeHashed"
            case false => fields.get(avroField.name()).get
          }
        }
      }.filter(_ != "toNotBeHashed")
    Row.fromSeq(orderedFieldsToBeHashed).hashCode()
  }

  /**
    * Format the row columns to fields as specified on the Avro Schema.
    */
  def format(): Map[String, Any] = {
    avroSchema.getFields.map {
      (field) => {
        val fieldName = field.name()
        val fieldValue = fieldName match {
          case "unique_id" => UID
          case "id" => UID
          case "version" => -1
          case "matched_ids" => sourceIDs.map(ds => (usedNameForOutput(ds._1) -> ds._2))
          case "matched_datasources" => matchedDataSources.map(usedNameForOutput(_))
          case "scoring_matched_datasources" => matchedDataSources.length
          case "scoring_is_parent_company" => ThirdPartyField.getValueFromMap(field, row, RCT, 0)
          case _ => ThirdPartyField.getMapValue(field, row)
        }
        (fieldName -> fieldValue)
      }
    }.toMap[String, Any]
  }

  /**
    * Make all the required computations to prepare the Third Party Fields to be
    * converted to [[Row]] object.
    */
  def prepare(): ContainerResult = {
    UID = calculateUID()
    fields = format()
    hash = calculateHash()
    fields = fields.updated("version", hash)

    ContainerResult(row = getAsRow(), thirdPartyIDs = findNewThirdPartyIDs(), uuids = UIDs,  sourceIDs = sourceIDs, uuid = UID)
  }

  /**
    * Identify the new ThirdPartyIDs that must be persisted on the mapping table.
    *
    * @return
    * [[Iterable[ThirdPartyID]]]
    */
  def findNewThirdPartyIDs(): Iterable[ThirdPartyID] = {
    UIDs.filter{
      case (dataSourceName, uid) => {
        uid match {
          case Some(x) => if (x.equals(UID)) false else true
          case None => true
        }
      }
    }.map {
        case (dataSourceName, uid) =>
          ThirdPartyID(usedNameForOutput(dataSourceName), sourceIDs.get(dataSourceName).get, UID)
      }
  }

  /**
    * Get the Third Party Fields as a Row Object.
    * The Row Object will be used to transform it afterwards to DataFrame.
    *
    * @return
    * [[Row]]
    */
  def getAsRow() = {
    val orderedFieldsValues = avroSchema.getFields
      .map((avroField) => fields.get(avroField.name()).getOrElse(null))
      .toSeq
    Row.fromSeq(orderedFieldsValues)
  }
}