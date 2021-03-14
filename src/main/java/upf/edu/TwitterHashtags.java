package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.storage.DynamoHashTagRepository;
import upf.edu.util.ConfigUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TwitterHashtags {

    public static void main(String[] args) throws InterruptedException, IOException {
        String propertiesFile = args[0];
        String language = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Example");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // This is needed by spark to write down temporary data
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        JavaDStream<Status> languageTweets = stream.filter(s -> s.getLang().equals(language)).filter(s -> s.getHashtagEntities().length > 0);
		JavaDStream<String> hashtags = languageTweets.flatMap(s -> Arrays.asList(s.getHashtagEntities()).iterator()).map(s -> s.getText());
		
		try{
		languageTweets.foreachRDD(s ->{ 
			List<Status> list_s = s.collect();
			DynamoHashTagRepository dynamoDBRepository = new DynamoHashTagRepository();
			for(int i=0; i<list_s.size();i++) {
					dynamoDBRepository.write(list_s.get(i));
			}
			});
		}
		catch(Exception e){
			System.out.println(e);
		}
		
		//DEBUG
		//hashtags.print(); 

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
