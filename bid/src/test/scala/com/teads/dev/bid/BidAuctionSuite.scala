package com.teads.dev.bid

import org.scalatest.{BeforeAndAfterAll, FeatureSpec, Matchers}

class BidAuctionSuite extends FeatureSpec with Matchers with BeforeAndAfterAll {
  override def beforeAll(): Unit = {}

  feature("Get winning price") {
    scenario("There are no bidding yet") {
      val bidAuction = new BidAuction(30)
      val lastMaxPriceBiddingMap: Map[String, Int] = Map()
      val winningPrice = bidAuction.getWinningPrice(lastMaxPriceBiddingMap)
      winningPrice shouldBe (30)
    }

    scenario("There are only one bidding") {
      val bidAuction = new BidAuction(30)
      val lastMaxPriceBiddingMap: Map[String, Int] = Map("1" -> 50)
      val winningPrice = bidAuction.getWinningPrice(lastMaxPriceBiddingMap)
      winningPrice shouldBe (50)
    }

    scenario("There are more than one bidding") {
      val bidAuction = new BidAuction(30)
      val lastMaxPriceBiddingMap: Map[String, Int] = Map("1" -> 50, "2" -> 32, "4" -> 21)
      val winningPrice = bidAuction.getWinningPrice(lastMaxPriceBiddingMap)
      winningPrice shouldBe (32)
    }
  }

  feature("Get current winner") {
    scenario("There are no bidding yet") {
      val bidAuction = new BidAuction(30)
      val lastMaxPriceBiddingMap: Map[String, Int] = Map()
      val currentWinner = bidAuction.getCurrentWinner(lastMaxPriceBiddingMap)
      currentWinner shouldBe (None)
    }

    scenario("There are only one bidding") {
      val bidAuction = new BidAuction(30)
      val lastMaxPriceBiddingMap: Map[String, Int] = Map("1" -> 50)
      val currentWinner = bidAuction.getCurrentWinner(lastMaxPriceBiddingMap)
      currentWinner.get.id shouldBe ("1")
      currentWinner.get.price shouldBe (50)
    }

    scenario("There are more than one bidding") {
      val bidAuction = new BidAuction(30)
      val lastMaxPriceBiddingMap: Map[String, Int] = Map("1" -> 50, "2" -> 32, "4" -> 21)
      val currentWinner = bidAuction.getCurrentWinner(lastMaxPriceBiddingMap)
      currentWinner.get.id shouldBe ("1")
      currentWinner.get.price shouldBe (32)
    }

  }

  feature("Bidding") {
    scenario("There are no bidding yet") {
      val bidAuction = new BidAuction(30)
      val bidding = new Buyer("1", 30, 1)
      bidAuction.bidding(bidding)

      bidAuction.biddingList.size shouldBe (1)
      bidAuction.lastMaxPriceBiddingMap.size shouldBe (1)
    }

    scenario("There are more than one bidding already") {
      val bidAuction = new BidAuction(30)
      val biddingList: List[Buyer] = List(Buyer("1", 30, 1), Buyer("2", 50, 1), Buyer("1", 80, 2))
      val lastMaxPriceBiddingMap: Map[String, Int] = Map("1" -> 80, "2" -> 50)
      bidAuction.biddingList ++= biddingList
      bidAuction.lastMaxPriceBiddingMap ++= lastMaxPriceBiddingMap

      val bidding = new Buyer("2", 90, 1)
      bidAuction.bidding(bidding)

      bidAuction.biddingList.size shouldBe (4)
      bidAuction.lastMaxPriceBiddingMap.size shouldBe (2)
    }

    scenario("bidding and looking for winner") {
      val biddingList: List[Buyer] = List(Buyer("1", 30, 1), Buyer("2", 50, 1), Buyer("1", 80, 2), Buyer("3", 90, 1))
      val bidAuction = new BidAuction(30)
      var winner: Buyer = new Buyer("NA",0,0)
      for (b <- biddingList) {
        bidAuction.bidding(b)
        winner = bidAuction.getCurrentWinner().get
      }

      winner.id shouldBe("3")
      winner.price shouldBe(80)

      bidAuction.biddingList.size shouldBe (4)
      bidAuction.lastMaxPriceBiddingMap.size shouldBe (3)
    }
  }


  override def afterAll(): Unit = {}

}
