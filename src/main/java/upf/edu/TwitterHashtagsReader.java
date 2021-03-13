package upf.edu;

import java.io.IOException;
import java.util.List;

import twitter4j.auth.OAuthAuthorization;
import upf.edu.model.HashTagCount;
import upf.edu.storage.DynamoHashTagRepository;
import upf.edu.util.ConfigUtils;

public class TwitterHashtagsReader {
	public static void main(String[] args) throws InterruptedException, IOException {
		String propertiesFile = args[0];
		String language = args[1];
		OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);
		
		DynamoHashTagRepository dynamoDB = new DynamoHashTagRepository();
		List<HashTagCount> hashtag_table = dynamoDB.readTop10(language);
		
		for(HashTagCount hashtag: hashtag_table){
			System.out.println(hashtag.toString());
		}
	}

}
