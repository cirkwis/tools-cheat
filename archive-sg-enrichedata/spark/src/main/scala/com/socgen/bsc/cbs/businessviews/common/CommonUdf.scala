package com.socgen.bsc.cbs.businessviews.common

import scala.collection.mutable.ArrayBuffer

object CommonUdf {

  def split(sep: String, limit: Int)(column: String): Seq[String] = {
    column match {
      case null => Seq.fill(limit)(null)
      case _ => {
        val columnValues: Array[String] = column.split(sep, -1)
        val splits: ArrayBuffer[String] = new ArrayBuffer[String](limit)
        for (i <- 0 until limit) {
          if (i < columnValues.length) {
            splits.append(columnValues(i))
          }
          else {
            splits.append(null)
          }
        }
        splits
      }
    }
  }

  def customFilter(filteredStr: List[String], includeFilter: Boolean) =
    (column: String, filteredColumn: String) => {
      filteredStr.contains(filteredColumn) match {
        case true => includeFilter match {
          case true => column
          case false => null
        }
        case false => includeFilter match {
          case true => null
          case false => column
        }
      }
    }

  def intervalMaper() = {
    (columnToMap: Int) => {
      if (columnToMap >= 3200 && columnToMap < 8200) "A"
      else if (columnToMap >= 8200 && columnToMap < 250000) "B"
      else if (columnToMap > 250000 && columnToMap < 1000000) "C"
      else if (columnToMap > 1000000) "D"
      else null
    }
  }

  def dateIdqMaper() = {
    (columnToMap: String) => {
      if (columnToMap != "") "Closed" else "Open"
    }
  }

  def sirenMaper() = {
    (columnToMap: String) => {
      if (columnToMap == "vide") null else columnToMap
    }
  }

  def customFilterAndExtract(filteredStr: List[String], includeFilter: Boolean, index: Int) =
    (column: String, filteredColumn: String) => {
      filteredStr.contains(filteredColumn) match {
        case true => includeFilter match {
          case true => column match {
            case "" => ""
            case null => null
            case _ => column(index).toString
          }
          case false => null
        }
        case false => includeFilter match {
          case true => null
          case false => column(index).toString
        }
      }
    }


  def concatenate(sep: String) =
    (fields: Seq[String]) => {
      def getValue(s: String): String = s match {
        case null => ""
        case x => x.replaceAll("\\s+", " ")
      }

      fields.reduce((s1, s2) => {
        val t = (getValue(s1), getValue(s2))
        t match {
          case ("", "") => ""
          case ("", t2) => t2
          case (t1, "") => t1
          case _ => t._1 + sep + t._2
        }
      })
    }


  def extractIndex(index: Int) =
    (s: String) => {
      s match {
        case null => null
        case "" => ""
        case _ => s(index - 1).toString
      }
    }

  //TODO put those UDFs on another space
  val sign = Map(
    "+" -> List("{", "A", "B", "C", "D", "E", "F", "G", "H", "I"),
    "-" -> List("}", "J", "K", "L", "M", "N", "O", "P", "Q", "R")
  )

  val mapOfLastCharacterAndValues = Map(
    "{" -> "0",
    "}" -> "0",
    "A" -> "1",
    "J" -> "1",
    "B" -> "2",
    "K" -> "2",
    "C" -> "3",
    "L" -> "3",
    "D" -> "4",
    "M" -> "4",
    "E" -> "5",
    "N" -> "5",
    "F" -> "6",
    "O" -> "6",
    "G" -> "7",
    "P" -> "7",
    "H" -> "8",
    "Q" -> "8",
    "I" -> "9",
    "R" -> "9"
  )

  val keyNotFound = "KEY_NOT_FOUND"

  def getMapOfLastCharacterAndValues(key: String): String = {
    var res = ""
    if (mapOfLastCharacterAndValues.contains(key)) {
      res = mapOfLastCharacterAndValues(key)
    } else {
      res = keyNotFound
    }
    res
  }

  def computeSign() = (s: String) => {
    val plus = sign("+")
    val minus = sign("-")
    if (s != null && s.length != 0) {
      if (plus.contains(s(s.length - 1).toString)) "+"
      else if (minus.contains(s(s.length - 1).toString)) "-"
      else null
    }
    else null
  }

  def extractValueAndConcatenate(to: Int) = (s: String) => {
    if (s == null || s.length <= 0) null
    else {
      val valueToConcatenate = getMapOfLastCharacterAndValues(s(s.length - 1).toString)
      if (valueToConcatenate.equals(keyNotFound)) {
        s
      } else {
        s.substring(0, to) ++ valueToConcatenate
      }
    }
  }

  def extractValueAndConcatenateWithKeyNotFound(to: Int) = (s: String) => {
    if (s == null || s.length <= 0) null
    else {
      val valueToConcatenate = getMapOfLastCharacterAndValues(s(s.length - 1).toString)
      if (valueToConcatenate.equals(keyNotFound)) {
        keyNotFound
      } else {
        s.substring(0, to) ++ valueToConcatenate
      }
    }
  }
}