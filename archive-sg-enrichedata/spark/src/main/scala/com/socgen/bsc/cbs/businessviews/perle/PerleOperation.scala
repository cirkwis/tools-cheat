package com.socgen.bsc.cbs.businessviews.perle

import org.apache.spark.sql.Row

/**
  * Created by A454489 on 23/02/2017.
  */
object PerleOperation {
  def checkNull(result: String): String = {
    var res = result
    try {
      res = result.replace("null", "").trim
      if (res == "")
        res = null
    } catch {
      case e: Exception => {
        println(e.getMessage)
      }
    }
    res
  }

  /**
    * Function: getFieldIfExist
    *
    * @param adr       : address type Row
    * @param fieldName : the field to check if it is contained in adr
    * @return the field value if exist or null if not
    */
  def getFieldIfExist(adr: Row, fieldName: String): String = {
    if (adr.schema.fields.map(a => a.name).contains(fieldName)) {
      if (adr.getAs(fieldName) != null)
        return adr.getAs(fieldName).toString
    }
    null
  }
}
