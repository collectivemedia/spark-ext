package com.collective.sparkext.example

import java.io.{PrintWriter, File}

import scala.util.Random

/**
 * Generate dummy dataset based on positive/negative predictors: site visitation log + geo location log
 */
object DataGenerator extends App with PositivePredictors with NegativePredictors {

  val (positive, negative) = Seq.fill(1000)(Random.alphanumeric.take(15).mkString).splitAt(100)

  val (pSites, pGeo, pResp) = generateDataset(positive, positivePredictors, negativePredictors, response = 1)
  val (nSites, nGeo, nResp) = generateDataset(negative, negativePredictors, positivePredictors, response = 0)

  // Write site impression log
  val sitesW = new PrintWriter(new File("sites.csv"))
  sitesW.println("cookie,site,impressions")
  (pSites ++ nSites).foreach(sitesW.println)
  sitesW.close()

  // Write geo impression log
  val geoW = new PrintWriter(new File("geo.csv"))
  geoW.println("cookie,lat,lon,impressions")
  (pGeo ++ nGeo).foreach(geoW.println)
  geoW.close()

  // Write response log
  val responseW = new PrintWriter(new File("response.csv"))
  responseW.println("cookie,response")
  (pResp ++ nResp).foreach(responseW.println)
  responseW.close()

  private def generateDataset(
    cookies: Seq[String],
    primaryPredictors: Predictors,
    secondaryPredictors: Predictors,
    response: Int,
    primaryImpMean: Int = 10,
    secondaryImpMean: Int = 3
  ): (Seq[String], Seq[String], Seq[String]) = {

    def impressions(mean: Int): Int = math.max(1, mean + (mean * rnd.nextGaussian()).toInt)

    val sites = cookies.flatMap { cookie =>
      val primary = primaryPredictors.sites(6).map((_, impressions(primaryImpMean)))
      val secondary = secondaryPredictors.sites(3).map((_, impressions(secondaryImpMean)))
      (primary ++ secondary) map { case (site, imp) => s"$cookie,$site,$imp" }
    }

    val geo = cookies.flatMap { cookie =>
      val primary = primaryPredictors.latLon(2).map((_, impressions(primaryImpMean)))
      val secondary = secondaryPredictors.latLon(1).map((_, impressions(secondaryImpMean)))
      (primary ++ secondary) map { case ((lat, lon), imp) => s"$cookie,$lat,$lon,$imp" }
    }

    val resp = cookies.map { cookie => s"$cookie,$response" }

    (sites, geo, resp)
  }

}

trait Predictors {

  def lat: Double
  def lon: Double
  def allSites: Seq[String]

  def sites(n: Int): Seq[String] =
    rnd.shuffle(allSites).take(1 + rnd.nextInt(n))

  def latLon(n: Int): Seq[(Double, Double)] =
    Seq.fill(1 + rnd.nextInt(n))((lat + 3 * rnd.nextGaussian(), lon + 3 * rnd.nextGaussian()))
}

trait PositivePredictors {

  val positivePredictors = new Predictors {

    // New York
    val lat = 40.7127
    val lon = 74.0059

    val allSites = Seq(
      "google.com", "facebook.com", "amazon.com",
      "youtube.com", "yahoo.com", "ebay.com", "wikipedia.org",
      "twitter.com", "craiglist.com", "reddit.com", "netflix.com",
      "live.com", "bing.com", "linkedin.com", "pinterest.com"
    )

  }
}

trait NegativePredictors {

  val negativePredictors = new Predictors {

    // Los Angeles
    val lat = 34.0500
    val lon = 118.2500

    val allSites = Seq(
      "imgur.com", "go.com", "tumblr.com", "espn.go.com",
      "cnn.com", "paypal.com", "chase.com", "instagram.com", "blogpost.com",
      "t.co", "msn.com", "imdb.com", "nytimes.com", "walmart.com",
      "huffingtonpost.com", "yelp.com", "diply.com"
    )

  }
}
