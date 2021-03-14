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
	//Standard DynamoDB configurations
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
	  //Create array of hashtagentities and get those entities from twitter
	  HashtagEntity hashtags_entities[] = tweet.getHashtagEntities();
	    
	  for(HashtagEntity h : hashtags_entities){
		  //GetItemSpecs and Item given PK hashtag
	      GetItemSpec hashtag = new GetItemSpec().withPrimaryKey("hashtag", h.getText());
	      
	      Item hashtag_item = dynamoDBtable.getItem(hashtag);
	      
	      if(hashtag_item != null){
	    	  //Case when the hashtag already in the table
	          List<Long> tweets = hashtag_item.getList("tweetId");
		      tweets.add(tweet.getId());
		       //Create item, meaning sum one to the counter
		      Item existHashtag = new Item().withPrimaryKey("hashtag", h.getText())
		        		.withString("language", tweet.getLang())
		                .withLong("counter", hashtag_item.getLong("counter") + 1L)
		                .withList("tweetId", tweets);
		        
		       dynamoDBtable.putItem(existHashtag);
	      }
	      
	      else{
	    	  //Create a new item and added to the table, put counter 1
	    	  List<Long> tweets = new ArrayList<Long>();
		      tweets.add(tweet.getId());
		      Item newHashtag = new Item().withPrimaryKey("hashtag", h.getText())
		        				  .withString("language", tweet.getLang())
		                          .withLong("counter", 1)
		                          .withList("tweetId", tweets);

		       dynamoDBtable.putItem(newHashtag);
	 
	      }
	      
	    }
  }

  @Override
  public List<HashTagCount> readTop10(String lang) {
	  
	  //Create two list hashtags and hashtags top 10
	  	List<HashTagCount> hashtags = new ArrayList<>();
	    List<HashTagCount> hashcount_top10 = new ArrayList<>();
	    
	    HashMap<String, Condition> sFilter = new HashMap<String, Condition>();
	    
	    //Create the condition to get tweets in the specified language
	    Condition c= new Condition()
	            .withComparisonOperator(ComparisonOperator.EQ)
	            .withAttributeValueList(new AttributeValue().withS(lang));
	    
	    //Perform the scan with the specified filter ( tweets in the same language)
	    sFilter.put("language", c);
	    ScanRequest sRequest = new ScanRequest("LSDS2020-TwitterHashtags").withScanFilter(sFilter);
	    ScanResult sResult = client.scan(sRequest);
	    
	    //Get the items in the scan
	    List<Map<String, AttributeValue>> table_items = sResult.getItems();
	    
	   //For each item, get the language and counter 
	    for(Map<String, AttributeValue> item : table_items){
	      GetItemSpec hashtag = new GetItemSpec().withPrimaryKey("hashtag", item.get("hashtag").getS());
	      Item hashtag_item = dynamoDBtable.getItem(hashtag);
	      
	      HashTagCount hashcount = new HashTagCount((String)hashtag_item.get("hashtag"),(String)hashtag_item.get("language"),((BigDecimal) hashtag_item.get("counter")).longValue());
	      
	      hashtags.add(hashcount);
	    }
	    
	    //sort with counter
	    hashtags.sort(new HashtagComparator());
	    Collections.reverse(hashtags);
	    
	    //add top elements to hashcount_top10
	    for(int i = 0; i < 10; i++){
	      hashcount_top10.add(hashtags.get(i));
	    }
	    
	    return hashcount_top10;
	  }
  

}
