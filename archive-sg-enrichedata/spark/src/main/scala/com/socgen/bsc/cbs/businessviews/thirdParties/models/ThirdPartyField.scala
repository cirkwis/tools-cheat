package com.socgen.bsc.cbs.businessviews.thirdParties.models

import com.socgen.bsc.cbs.businessviews.common.LoggingSupport
import org.apache.avro.Schema
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import com.socgen.bsc.cbs.businessviews.thirdParties.config.DataSourceName._
import com.socgen.bsc.cbs.businessviews.thirdParties.config.{DataSourceName, ThirdPartiesBvConfig}

import scala.collection.JavaConversions._
import com.socgen.bsc.cbs.businessviews.thirdParties.tools.DataFrameLoaderWithPrefix

object ThirdPartyField extends LoggingSupport {

  /**
    * Identify the type for an Avro Field.
    * e.g Avro field:
    * {
    * "name": "acquiring_le_identifier",
    * "type": [
    * "null",
    * {
    * "type": "map",
    * "values": "string"
    * }
    * ]
    * }
    * The Avro field type is [String].
    *
    * @param field
    * [[Schema.Field]]
    * @return
    * [[Schema.Type]]
    */
  def getType(field: Schema.Field): Schema.Type = {
    field.schema().getTypes.get(1).getValueType().toString() match {
      case a if a.startsWith("string", 1) => Schema.Type.STRING
      case b if b.startsWith("int", 1) => Schema.Type.INT
      case c if c.startsWith("long", 1) => Schema.Type.LONG
      case d if d.startsWith("double", 1) => Schema.Type.DOUBLE
      case e if e.startsWith("boolean", 1) => Schema.Type.BOOLEAN
      case f if f.startsWith("array", 9) => Schema.Type.ARRAY
      case _ => {
        debug("Field Type not identified [" + field + "]")
        null
      }
    }
  }

  /**
    * Construct the value for the Avro [[Schema.Field]] passed in parameter.
    *
    * We iterate each "from" node, to retrieve its value.
    * At the end, we get a [[Map[String, Any]]] result where :
    * - String: represents the dataSource name.
    * - Any: represents the dataSource field value, retrieved from [[Row]] object.
    *
    * e.g "from" node:
    * "from": [
    * {
    * "dataSource": "rct",
    * "fieldName": "acquiring_le_identifier"
    * }
    * ]
    *
    * @param field
    * [[Schema.Field]]
    * @param row
    * [[Row]]
    * @return
    * [[Map[String, Any]]]
    */
  def getMapValue(field: Schema.Field, row: Row): Map[String, Any] = {
    val from = field.getJsonProp("from")
    if (from == null) {
      debug(s"debug: [AvroField=$field.name], It doesn't specified how the field is fed!!")
      return null
    }

    val debugMessage = new StringBuilder(s"debug: [AvroField=${field.name}] is fed from: { \n")

    val fieldValues = from.getElements.map {
      (node) => {
        val dataSourceName = DataSourceName.withName(node.get("dataSource").getTextValue)
        // TODO: to move to an other class
        val fieldName = DataFrameLoaderWithPrefix.colPrefixed(dataSourceName, node.get("fieldName").getTextValue)

        val fieldValue = isExistOnRowSchema(fieldName, row.schema) match {
          case true => {
            val value = row.get(row.fieldIndex(fieldName))
            getType(field) match {
              case Schema.Type.INT => toInt(value).getOrElse(null)
              case Schema.Type.BOOLEAN => toBoolean(value).getOrElse(null)
              case Schema.Type.LONG => toLong(value).getOrElse(null)
              case Schema.Type.DOUBLE => toDouble(value).getOrElse(null)
              case _ => value
            }
          }
          case false => {
            debugMessage.append("[Not Retrieved]. The field doesn't exist from Joins. ")
            null
          }
        }

        debugMessage.append(s"- $dataSourceName -> $fieldName : $fieldValue \n")
        (ThirdPartiesBvConfig.usedNameForOutput(dataSourceName) -> fieldValue)
      }
    }.toMap[String, Any]
    debugMessage.append("}\n")
    debug(debugMessage.toString())

    cleanMapValue(fieldValues)
  }

  def getValueFromMap(field: Schema.Field, row: Row, dataSourceName: DataSourceName, defaultValue: Any): Any = {
    val result = getMapValue(field, row)
    result match {
      case null => defaultValue
      case _ => result.getOrElse(ThirdPartiesBvConfig.usedNameForOutput(dataSourceName), defaultValue)
    }
  }

  /**
    * Cleaning the Field Result.
    * e.g:
    * Clean this field result
    * "acquiring_le_identifier": {
    * "rct": "ACW5290",
    * "dun": null
    * }
    * to ->
    * "acquiring_le_identifier": {
    * "rct": "ACW5290"
    * }
    *
    * @param field
    * [[Map[String, Any]]]
    * @return
    * [[Map[String, Any]]]
    */
  def cleanMapValue(field: Map[String, Any]): Map[String, Any] = {
    val result = field.filter(_._2 != null)

    if (result.isEmpty)
      return null

    result
  }

  /**
    * Verify if the Avro Node fieldName exists on the [[Row]] object.
    *
    * @param fieldName
    * @param rowSchema
    * @return
    */
  def isExistOnRowSchema(fieldName: String, rowSchema: StructType): Boolean = {
    rowSchema.fieldNames.contains(fieldName)
  }

  /**
    * Convert a String to Int
    *
    * @param value
    * @return
    */
  def toInt(value: Any): Option[Int] = {
    try {
      Some(value.toString.toInt)
    } catch {
      case e: NumberFormatException => None
      case e: NullPointerException => None
    }
  }

  /**
    * Convert a String to Double
    *
    * @param value
    * @return
    */
  def toDouble(value: Any): Option[Double] = {
    try {
      Some(value.toString.toDouble)
    } catch {
      case e: NumberFormatException => None
      case a: NullPointerException => None
    }
  }

  /**
    * Convert a String to Boolean
    *
    * @param value
    * @return
    */
  def toBoolean(value: Any): Option[Boolean] = {
    try {
      Some(value.toString.toBoolean)
    } catch {
      case e: NumberFormatException => None
      case e: NullPointerException => None
    }
  }

  /**
    * Convert a String to Long
    *
    * @param value
    * @return
    */
  def toLong(value: Any): Option[Long] = {
    try {
      Some(value.toString.toLong)
    } catch {
      case e: NumberFormatException => None
      case e: NullPointerException => None
    }
  }
}