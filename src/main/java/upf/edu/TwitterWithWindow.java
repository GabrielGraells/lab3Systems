package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import upf.edu.util.LanguageMapUtils;

import java.io.IOException;

public class TwitterWithWindow {
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String input = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter with windows");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(30));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // Read the language map file as RDD
        final JavaRDD<String> languageMapLines = jsc
                .sparkContext()
                .textFile(input);
        final JavaPairRDD<String, String> languageMap = LanguageMapUtils
                .buildLanguageMap(languageMapLines);

        // create an initial stream that counts language within the batch (as in the previous exercise)
        final JavaPairDStream<String, Integer> languageCountStream = stream
                .transformToPair(element -> element
                        .mapToPair(tweet -> new Tuple2<String, Integer>(tweet.getLang(), 1))
                        .join(languageMap)
                        .mapToPair(langTweet -> new Tuple2<String, Integer>(langTweet._2._2, langTweet._2._1))
                        .reduceByKey((a,b) -> a+b)
                ); // IMPLEMENT ME

        // Prepare output within the batch
        final JavaPairDStream<Integer, String> languageBatchByCount = languageCountStream
                .mapToPair(x -> new Tuple2<Integer, String>(x._2,x._1))
                .transformToPair(rdd -> rdd
                .sortByKey(false)); // IMPLEMENT ME

        // Prepare output within the window
        final JavaPairDStream<Integer, String> languageWindowByCount = stream
                .window(Durations.seconds(60))
                .transformToPair((element -> element
                    .mapToPair(tweet -> new Tuple2<String, Integer>(tweet.getLang(), 1))
                    .join(languageMap)
                    .mapToPair(langTweet -> new Tuple2<String, Integer>(langTweet._2._2, langTweet._2._1))
                    .reduceByKey((a,b) -> a+b)))
                    .mapToPair(x -> new Tuple2<Integer, String>(x._2, x._1))
                    .transformToPair(rdd -> rdd
                        .sortByKey(false)); // IMPLEMENT ME

        // Print first 15 results for each one
        languageBatchByCount.print(15);
        languageWindowByCount.print(15);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
