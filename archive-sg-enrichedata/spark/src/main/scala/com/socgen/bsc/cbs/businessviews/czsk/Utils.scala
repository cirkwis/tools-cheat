package com.socgen.bsc.cbs.businessviews.czsk

import java.util.Calendar

/**
  * Created by X156170 on 17/10/2017.
  * This class contains functions which are used for the RN Businessview
  */
object Utils {


  val unknown = "Unknown"

  /**
    * @return a function which extract the currency for the RN Businessview
    *
    */
  def extractCurrency = (s: String) => if (s == unknown) unknown else if (s == null) null else s.takeRight(3)

  /**
    *
    * @return a function which extract the high range for RN Businessview from a string
    */
  def extractHighRange = {
    (s: String) =>
      if (s == unknown) unknown
      else if (s == null) null
      else if(s.split("-").size == 2) {
        val checkList = List('C', 't', 'e')
        s.split("-")(1).replaceAll("\\s", "").takeWhile(!checkList.contains(_))
      }else {
        unknown
      }
  }

  /**
    *
    * @return a function which extract the low range for RN Businessview from a string
    */
  def extractLowRange = {

    (s: String) =>
      if (s == unknown) unknown
      else if (s == null) null
      else s.split("-")(0).replaceAll("\\s", "")
  }

  /**
    *
    * @return the current year
    */
  def getCurrentYear = Calendar.getInstance().get(Calendar.YEAR)

  /**
    *
    * @return a function which multiply by thousand the high and the low sale if the currency is EUR
    */
  def byThousand = (sales: String, currency: String) => {
    if (sales == unknown) unknown
    else if (sales == null) null
    else{
      if (currency == "EUR") sales.concat("000") else sales
    }

  }

}
