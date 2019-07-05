package bd.karanjag.kafka.producer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.twitter.hbc.*;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.Location.Coordinate;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import bd.karanjag.kafka.config.TwitterConfiguration;

public class KafkaTwitterProducer {
	
	public Client client;
	BlockingQueue<String> queue;
	
	
	public KafkaTwitterProducer() {
		
		Authentication authen  = new OAuth1(
				TwitterConfiguration.CONSUMER_KEY,
				TwitterConfiguration.CONSUMER_SECRET,
				TwitterConfiguration.ACCESS_TOKEN,
				TwitterConfiguration.ACCESS_SECRET
				);
		
		StatusesFilterEndpoint tracking = new StatusesFilterEndpoint();
		//new double[][]{ {68.116667 ,8.066667}, {97.416667,37.100000} } Bangalore
		Coordinate v1 = new Coordinate(68.116667 ,8.066667);
		Coordinate v2 = new Coordinate(97.416667,37.100000);
		Location india = new Location(v1,v2);
		tracking.locations(Lists.newArrayList(india));
		
		queue = new LinkedBlockingQueue<>(10000);
		
		ClientBuilder cb = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(Constants.STREAM_HOST)
				  .authentication(authen)
				  .endpoint(tracking)
				  .processor(new StringDelimitedProcessor(queue));   
		client = cb.build();
		
	}
	

}
