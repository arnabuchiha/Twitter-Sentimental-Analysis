//import org.apache.spark.SparkConf
import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

import scala.util.Try
import com.mongodb.spark.sql._
import org.apache.spark.streaming.twitter.TwitterUtils
import SentimentalAnalysis.detectSentiment
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.writer.{SqlRowWriter, TTLOption, WriteConf}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark
import com.mongodb.spark.config._
import com.mongodb.spark._
import org.bson.Document
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
  val filters=Array("Electrolux")
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


    val conf = new SparkConf().setAppName("senti_analyze").setMaster("local[*]")
    conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/spark.tweetanalysis")
    conf.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/spark.tweetanalysis")
    val sc=new SparkContext(conf)
    import org.bson.Document

    val ssc = new StreamingContext(sc, Seconds(10))
//    val sc = spark.SparkContext.getOrCreate()
//    sc.cassandraTable("twitter","twitter_table")
    val tweets = TwitterUtils.createStream(ssc, None, filters)
    tweets.print()
//    try {
//      val rdd = sc.parallelize(Seq(tweetWithSentiment))
//      MongoSpark.save(rdd)
//    }finally{
//      print("Done")
//    }
//    rdd.saveToMongoDB()

//    tweets.foreachRDD({ rdd =>
//      val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext)
//      import sparkSession.implicits._
//      rdd.map(t => {
//        (t.getUser.getScreenName.toString, detectSentiment(t.getText))
//      }).toDF().write.mode("append").mongo()
//    })
    tweets.foreachRDD({ rdd =>
      val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext)
      import sparkSession.implicits._
      rdd.map(t => {

        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        Document.parse(mapper.writeValueAsString(Map(
          "user" -> t.getUser.getScreenName,
          "created_at" -> t.getCreatedAt.toInstant.toString,
          "location" -> Option(t.getGeoLocation).map(geo => {
            s"${geo.getLatitude},${geo.getLongitude}"
          }),
          "text" -> t.getText,
          "hashtags" -> t.getHashtagEntities.map(_.getText),
          "retweet" -> t.getRetweetCount,
          "language" -> t.getLang,
          "sentiment" -> detectSentiment(t.getText).toString
        )))
      }).saveToMongoDB()
    })
//    tweets.foreachRDD((rdd,time)=>
//      rdd.map(t=>(System.out.println(t.getText)))
//    )
    ssc.start()
    ssc.awaitTermination()

  }
  object SparkSessionSingleton {

    @transient private var instance: SparkSession = _

    def getInstance(sparkContext: SparkContext): SparkSession = {
      if (Option(instance).isEmpty) {
        instance = SparkSession.builder().getOrCreate()
      }
      instance
    }
  }
  def detectLanguage(text: String) : Any = {

    Try {
      System.out.println(text)
    }.getOrElse("unknown")

  }
}
