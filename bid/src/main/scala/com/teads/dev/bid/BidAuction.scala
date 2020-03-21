package com.teads.dev.bid

import scala.collection.mutable.ListBuffer

class BidAuction(initialPrice: Int) {
  var biddingList = new ListBuffer[Buyer]()
  var lastMaxPriceBiddingMap: Map[String, Int] = Map()

  /** *
    * The buyer winning the auction is the one with the highest bid above or equal to the reserve price.
    *
    * @param lastMaxPriceBiddingMap
    * @return
    */
  def getCurrentWinner(lastMaxPriceBiddingMap: Map[String, Int]): Option[Buyer] = {
    if (!lastMaxPriceBiddingMap.isEmpty) {
      val winningPrice = getWinningPrice(lastMaxPriceBiddingMap)
      val winnerId = lastMaxPriceBiddingMap.maxBy(f => f._2)._1
      println(s"The current winner is the buyer with id:$winnerId and winning price: $winningPrice")
      Option(Buyer(winnerId, winningPrice, 0))
    }
    else {
      println(s"No bidding......")
      None
    }
  }

  def getCurrentWinner():Option[Buyer] ={
    getCurrentWinner(this.lastMaxPriceBiddingMap)
  }

  /**
    * The winning price is the highest bid price from a non-winning buyer above the reserve price
    * (or the reserve price if none applies)
    */
  def getWinningPrice(lastMaxPriceBiddingMap: Map[String, Int]): Int = {
    val numberOfBidding = lastMaxPriceBiddingMap.size
    if (numberOfBidding == 1)
      lastMaxPriceBiddingMap.valuesIterator.max
    else if (numberOfBidding > 1)
      (lastMaxPriceBiddingMap - lastMaxPriceBiddingMap.maxBy(f => f._2)._1).valuesIterator.max
    else
      this.initialPrice
  }

  def bidding(bidding: Buyer): Unit = {
    if (biddingList.isEmpty && lastMaxPriceBiddingMap.isEmpty && bidding.price >= this.initialPrice) {
      biddingList += bidding
      lastMaxPriceBiddingMap += (bidding.id -> bidding.price)
    }
    else {
      val currentMaxPrice = lastMaxPriceBiddingMap.valuesIterator.max
      if (bidding.price < currentMaxPrice)
        println("Bidding is not valid")
      else if (bidding.time == -1) { // the buyer want to leave the auction
        biddingList = biddingList.filter(f => f.id != bidding.id)
        lastMaxPriceBiddingMap -= bidding.id
      }
      else {
        biddingList += bidding
        lastMaxPriceBiddingMap += (bidding.id -> bidding.price)
      }
    }
  }
}