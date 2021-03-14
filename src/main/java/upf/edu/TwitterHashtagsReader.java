package upf.edu;

import java.io.IOException;
import java.util.List;

import twitter4j.auth.OAuthAuthorization;
import upf.edu.model.HashTagCount;
import upf.edu.storage.DynamoHashTagRepository;
import upf.edu.util.ConfigUtils;

public class TwitterHashtagsReader {
	public static void main(String[] args){
		
		String propertiesFile = args[0];
		String language = args[1];
		
		try {
			OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		DynamoHashTagRepository dynamoDB = new DynamoHashTagRepository();
		List<HashTagCount> hashtag_table = dynamoDB.readTop10(language);
		
		for(int i=0;i<hashtag_table.size();i++){
			System.out.println("#############"+hashtag_table.get(i).toString()+"###########"+"\n");
		}
	}

}
