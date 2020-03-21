package com.socgen.bsc.cbs.businessviews.thirdParties.avro

import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType

object AvroFunctions {
  /**
    * The method converts an Avro Schema, loaded from the resources/ path, to a structType.
    *
    * @param schemaNames
    * Avro Schema file name
    * @return
    * StructType object
    */
  def getStructTypeFromAvroFile(schemaPath: String, schemaNames: String*): StructType = {
    val schema: Schema = getAvroSchema(schemaPath, schemaNames: _*)
    getStructTypeFromAvroSchema(schema)
  }

  def getStructTypeFromAvroSchema(schema: Schema): StructType = {
    val dataType = SchemaConverter.toSqlType(schema).dataType
    dataType.asInstanceOf[StructType]
  }

  def getAvroSchema(schemaPath: String, schemaNames: String*): Schema = {
    val parser = new Schema.Parser()
    var schema: Schema = null
    schemaNames.foreach(schemaName => {
      val schemaInput = this.getClass.getClassLoader.getResourceAsStream(schemaPath + "/" + schemaName)
      if(schemaInput == null)
        throw new RuntimeException(s"Avro files missing $schemaName !")
      schema = parser.parse(schemaInput)
    })
    schema
  }
}
