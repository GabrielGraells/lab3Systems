package upf.edu.storage;

import twitter4j.Status;
import twitter4j.HashtagEntity;
import upf.edu.model.HashTagCount;
import upf.edu.model.HashtagComparator;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.*;


public class DynamoHashTagRepository implements IHashtagRepository, Serializable {
	final static String endpoint = "dynamodb.us-east-1.amazonaws.com";
	final static String region = "us-east-1";
	  final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
	          .withEndpointConfiguration(
	                  new AwsClientBuilder.EndpointConfiguration(endpoint, region)
	          ).build();
	  final DynamoDB dynamoDB = new DynamoDB(client);
	  final Table dynamoDBtable = dynamoDB.getTable("LSDS2020-TwitterHashtags");
	  
	  

  @Override
  public void write(Status tweet) {
	  HashtagEntity hashtags[] = tweet.getHashtagEntities();
	    for(HashtagEntity h : hashtags){
	      GetItemSpec getItem = new GetItemSpec().withPrimaryKey("hashtag", h.getText(), "language", tweet.getLang());
	      Item outcome = dynamoDBtable.getItem(getItem);
	      if(outcome == null){
	        List<Long> tweets = new ArrayList<Long>();
	        tweets.add(tweet.getId());
	        Item newItem = new Item().withPrimaryKey("hashtag", h.getText(), "language", tweet.getLang())
	                          .withLong("counter", 1)
	                          .withList("tweetId", tweets);
	        try {
	          dynamoDBtable.putItem(newItem);
	        }
	        catch (Exception e){
	          System.out.println(e);
	        }
	      }
	      else{
	        List<Long> tweets = outcome.getList("tweetId");
	        tweets.add(tweet.getId());
	        
	        Item newItem = new Item().withPrimaryKey("hashtag", h.getText(), "language", tweet.getLang())
	                .withLong("counter", outcome.getLong("counter") + 1L)
	                .withList("tweetId", tweets);
	        try {
	          dynamoDBtable.putItem(newItem);
	        }
	        catch (Exception e){
	          System.out.println(e);
	        }
	      }
	    }
  }

  @Override
  public List<HashTagCount> readTop10(String lang) {
	    HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
	    
	    Condition c= new Condition()
	            .withComparisonOperator(ComparisonOperator.EQ)
	            .withAttributeValueList(new AttributeValue().withS(lang));
	    
	    scanFilter.put("language", c);
	    ScanRequest scanRequest = new ScanRequest("LSDS2020-TwitterHashtags").withScanFilter(scanFilter);
	    ScanResult scanResult = client.scan(scanRequest);
	    
	    List<Map<String, AttributeValue>> items = scanResult.getItems();
	    List<HashTagCount> hashtags = new ArrayList<>();
	    
	    for(Map<String, AttributeValue> it : items){
	      GetItemSpec getItem = new GetItemSpec().withPrimaryKey("hashtag", it.get("hashtag").getS(), "language", it.get("language").getS());
	      Item outcome = dynamoDBtable.getItem(getItem);
	      HashTagCount htc = new HashTagCount((String)outcome.get("hashtag"),(String)outcome.get("language"),((BigDecimal) outcome.get("counter")).longValue());
	      hashtags.add(htc);
	    }
	    
	    hashtags.sort(new HashtagComparator());
	    List<HashTagCount> result = new ArrayList<>();
	    
	    for(int i = 0; i < 10; i++){
	      result.add(hashtags.get(i));
	    }
	    return result;
	  }
  

}
