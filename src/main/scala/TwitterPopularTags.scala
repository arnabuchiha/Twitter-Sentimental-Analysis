//import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._
import org.apache.spark.SparkContext._
import scala.util.Try
import org.apache.spark.streaming.twitter.TwitterUtils
import SentimentalAnalysis.detectSentiment
object TwitterPopularTags {
  def main(args: Array[String]) {

//    if (args.length < 4) {
//      System.err.println("Usage: TwitterSentimentAnalysis <consumer key> <consumer secret> " +
//        "<access token> <access token secret> [<filters>]")
//      System.exit(1)
//    }
    val consumerKey="yo8mYPmefJ82Lt5pqPz2JiteT"
    val consumerSecret="e8L7dsV5qiQOSbkj3iY6XW25DLh5RG0JQWkkhUX3wHzBFAdu5a"
    val accessToken="4884998369-5Sni2mAIauDDtZAOAMHYhd2R2khYS3iZ4xrK9VL"
    val accessTokenSecret="Qa7uJ9CTJhOq3GRIDnT2rxSMNGTKe9k1RzEbV7ZHcHhrZ"
    LogUtils.setStreamingLogLevels()

//    DetectorFactory.loadProfile("src/main/resources/profiles")

//    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
//    val filters = args.takeRight(args.length - 4)
  val filters=Array("Trump")
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val conf = new SparkConf().setAppName("senti_analyze").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(1))

    val tweets = TwitterUtils.createStream(ssc, None, filters)

    tweets.print()
    tweets.foreachRDD{(rdd, time) =>
      rdd.map(t => {
        Map(
          "user"-> t.getUser.getScreenName,
          "created_at" -> t.getCreatedAt.toInstant.toString,
          "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
          "text" -> t.getText,
          "hashtags" -> t.getHashtagEntities.map(_.getText),
          "retweet" -> t.getRetweetCount,
          "language" -> t.getLang,
          "sentiment" -> detectSentiment(t.getText).toString
        )
      })
    }
//    tweets.foreachRDD((rdd,time)=>
//      rdd.map(t=>(System.out.println(t.getText)))
//    )
    ssc.start()
    ssc.awaitTermination()

  }

  def detectLanguage(text: String) : Any = {

    Try {
      System.out.println(text)
    }.getOrElse("unknown")

  }
}
