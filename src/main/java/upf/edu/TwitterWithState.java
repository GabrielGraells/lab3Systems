package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;

import java.io.IOException;
import java.util.List;

public class TwitterWithState {
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String language = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter With State");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // create a simpler stream of <user, count> for the given language
        final JavaPairDStream<String, Integer> tweetPerUser = stream
        		.filter(a -> a.getLang().equals(language))
        		.mapToPair(b -> new Tuple2<String,Integer>(b.getUser().getScreenName(), 1))
        		.reduceByKey((c,d) -> c+d);
        
        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
        		(values, state) -> {
        			Integer newSum = state.or(0);
        			for(int i: values) 
        			{
        				newSum += i;
        			}
        			return Optional.of(newSum);
        		};
        		
        JavaPairDStream<String, Integer> rCounts = tweetPerUser.updateStateByKey(updateFunction);
        		

        // transform to a stream of <userTotal, userName> and get the first 20
        final JavaPairDStream<Integer, String> tweetsCountPerUser = rCounts
        		.mapToPair(x -> new Tuple2<Integer, String>(x._2, x._1))
        		.transformToPair(myRDD -> myRDD.sortByKey(false));
        
        tweetsCountPerUser.print();

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}