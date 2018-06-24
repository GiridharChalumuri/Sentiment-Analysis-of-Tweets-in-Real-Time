package com.assignment3

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext }
import twitter4j.Status
import twitter4j.FilterQuery
import org.apache.log4j._
import edu.stanford.nlp.sentiment._
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{ Annotation, StanfordCoreNLP }
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import java.util.Properties
import scala.collection.JavaConversions._
import com.assignment3.DetectSentiment._


object  Sentiment{

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = new SparkConf().setAppName("Question4")

    if (!spark.contains("spark.master")) {

      spark.setMaster("local[*]")
    }

    val filters = args.takeRight(args.length)

    val ssc = new StreamingContext(spark, Seconds(1))
    val Twitter_stream = TwitterUtils.createStream(ssc, None, filters)

    val tweets = Twitter_stream.filter(x => {
      x.getLang() == "en" && x.getGeoLocation() != null
    })
    
    val filtered_Tweets = tweets.filter(f => {
      val status = f.getText().toLowerCase()
      (status contains "obama") || (status contains "trump")
//      true
    })



    val x = filtered_Tweets.foreachRDD { (rdd, time) =>
      rdd.map(f => {
        val text = f.getText

        if (text.length() > 0) {
          val p = detectSentiment(text)
          val r = p match {
            case r if r <= 0.0 => "Can not decide"
            case r if r < 1.0  => "VERY_NEGATIVE"
            case r if r < 2.0  => "NEGATIVE"
            case r if r < 3.0  => "NEUTRAL"
            case r if r < 4.0  => "POSITIVE"
            case r if r < 5.0  => "VERY_POSITIVE"
            case r if r > 5.0  => "Can not decide"

          }
          text -> r
        } else {
          "" -> ""
        }

      }).filter(_._1 != "").foreach(println)

    }
    ssc.start()

    ssc.awaitTermination()
  }
}
    
    