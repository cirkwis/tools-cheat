package com.socgen.bsc.cbs.businessviews.infogreffe
import org.apache.spark.sql.Row



/**
  * Created by Guy Stephane Manganneau  on 19/12/2016.
  */
object InfogreffeBvTools {

  type FieldByYear = List[(String,Int)]

  def getTheMostUpdateFields(l1: FieldByYear, l2: FieldByYear): FieldByYear = {
    val zip = l1 zip l2 //TODO : pattern matching
    for (t <- zip) yield {
      if (t._1._2 > t._2._2 && t._1._1 == null) (t._2._1, t._2._2)
      else if (t._2._2 > t._1._2 && t._2._1 == null) (t._1._1, t._1._2)
      else if (t._1._2 > t._2._2) (t._1._1, t._1._2)
      else if (t._2._2 > t._1._2) (t._2._1, t._2._2)
      else (null, 0)
    }
  }

  def mapElementsOfListWithTheLast(r: Row): (String, List[(String, Int)]) = {
    val length = r.length
    val last = r.getInt(length - 1) //TODO : simplify
    (r.getString(0), (for (e <- r.toSeq.slice(1, length - 1)) yield (e.asInstanceOf[String], last)).toList)
  }
}
