package com.socgen.bsc.cbs.businessviews.avox

/**
  * Created by X156170 on 23/11/2017.
  */
object Utils {

  val sectionCodeMap = Map(
    "1" -> "A", "2" -> "A", "3" -> "A", "5" -> "B", "6" -> "B", "7" -> "B", "8" -> "B",
    "9" -> "B", "10" -> "CA", "11" -> "CA", "12" -> "CA", "13" -> "CB", "14" -> "CB", "15" -> "CB",
    "16" -> "CC", "17" -> "CC", "18" -> "CC", "19" -> "CD", "20" -> "CE", "21" -> "CF", "22" -> "CG",
    "23" -> "CG", "24" -> "CH", "25" -> "CH", "26" -> "CI", "27" -> "CJ", "28" -> "CK", "29" -> "CL",
    "30" -> "CL", "31" -> "CM", "32" -> "CM", "33" -> "CM", "35" -> "D", "36" -> "E", "37" -> "E",
    "38" -> "E", "39" -> "E", "41" -> "F", "42" -> "F", "43" -> "F", "45" -> "G", "46" -> "G",
    "47" -> "G", "49" -> "H", "50" -> "H", "51" -> "H", "52" -> "H", "53" -> "H", "55" -> "I",
    "56" -> "I", "58" -> "JA", "59" -> "JA", "60" -> "JA", "61" -> "JB", "62" -> "JC", "63" -> "JC",
    "64" -> "K", "65" -> "K", "66" -> "K", "68" -> "L", "69" -> "MA", "70" -> "MA", "71" -> "MA",
    "72" -> "MB", "73" -> "MC", "74" -> "MC", "75" -> "MC", "77" -> "N", "78" -> "N", "79" -> "N",
    "80" -> "N", "81" -> "N", "82" -> "N", "84" -> "O", "85" -> "P", "86" -> "QA", "87" -> "QB",
    "88" -> "QB", "90" -> "R", "91" -> "R", "92" -> "R", "93" -> "R", "94" -> "S", "95" -> "S",
    "96" -> "S", "97" -> "T", "98" -> "T", "99" -> "U"
  )


  def formatCodeNace = (s: String) => s match {
    case null => null
    case s: String => s.replace(".", "")
  }

  def extract2FirstChar = (s: String) => s match {
    case null => null
    case _: String => {
      if (s.length >= 2) s.charAt(0).toString + s.charAt(1).toString
      else {
        null
        //throw new Exception(s"$s is not long enough to extract 2 first ")
      }
    }
  }

  def getSectionCode = (s: String) => s match {
    case null => null
    case _ => {
      if (sectionCodeMap.contains(s)) sectionCodeMap(s)
      else {
        null
        //throw new Exception(s"The key $s is not in the sectioncode's Map")
      }
    }
  }

  def getDivisionCode = (s: String) => s match {
    case null => null
    case _ => {
      if (s.length >= 2) s.charAt(0).toString + s.charAt(1).toString
      else {
        null
        //throw new Exception(s"Nace code :$s is not long enough, it should be long at least 2")
      }
    }
  }

  def getGroupCode = (s: String) => s match {
    case null => null
    case _ => {
      if (s.length >= 3) (getDivisionCode(s) + s.charAt(2).toString)
      else {
        null
       // throw new Exception(s"Nace code :$s is not long enough, it should be long at least 4")
      }
    }
  }

  def getClassCode = (s: String) => s match {
    case null => null
    case _ => {
      if (s.length >= 4) (getGroupCode(s) + s.charAt(3).toString)
      else {
        null
        //throw new Exception(s"Nace code :$s is not long enough, it should be long at least 5")
      }
    }
  }

  def getSousClassCode = (s: String) => {
    s match {
      case null => null
      case _ => {
        if (s.length >= 5)
          (getClassCode(s) + s.substring(4))
        else if (s.length == 4)
          (getClassCode(s) + "00")
        else {
          null
          //throw new Exception(s"Nace code :$s is not long enough, it should be long at least 5")
        }
      }
    }
  }

}
