package bd.karanjag.kafka.producer;

import java.util.ArrayList;
import java.util.Properties;
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

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import bd.karanjag.kafka.config.KafkaConfiguration;
import bd.karanjag.kafka.config.TwitterConfiguration;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

public class TwitterClient {
	
	public Client client;
	BlockingQueue<String> queue;
	
	
	public TwitterClient() {
		
		Authentication authen  = new OAuth1(
				TwitterConfiguration.CONSUMER_KEY,
				TwitterConfiguration.CONSUMER_SECRET,
				TwitterConfiguration.ACCESS_TOKEN,
				TwitterConfiguration.ACCESS_SECRET
				);
		
		StatusesFilterEndpoint tracking = new StatusesFilterEndpoint();
		//new double[][]{ {68.116667 ,8.066667}, {97.416667,37.100000} } India
		Coordinate v1 = new Coordinate(68.116667 ,8.066667);
		Coordinate v2 = new Coordinate(97.416667,37.100000);
		Location india = new Location(v1,v2);
		ArrayList<String> terms = new ArrayList<>();
		terms.add("Big Data");
		terms.add("-filter:retweets");
		terms.add("-filter:media");
		terms.add("-filter:links");
		tracking.locations(Lists.newArrayList(india));
		tracking.languages(Lists.newArrayList("en"));
		tracking.trackTerms(terms);
		
		queue = new LinkedBlockingQueue<>(10000);
		
		ClientBuilder cb = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(Constants.STREAM_HOST)
				  .authentication(authen)
				  .endpoint(tracking)
				  .processor(new StringDelimitedProcessor(queue));   
		client = cb.build();
		
	}
	
	private Producer<Long,String> getproducer(){
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
		props.put(ProducerConfig.ACKS_CONFIG, "1"); //wait for ack only from receiving broker
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}
	
	public void run() {
		client.connect();
		try(Producer<Long,String> prod = getproducer()){
			while(true) {
				Status status = TwitterObjectFactory.createStatus(queue.take()); //Using Twitter4J to parse the JSON string into a POJO
				System.out.println("@"+status.getUser().getName()+ ":"+status.getText()+"\n####################\n");
				long key = status.getId();
				String msg = status.toString();
				ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(KafkaConfiguration.TOPIC,key, msg);
				prod.send(record);		//need to add callback
				
			}
		}
		catch(InterruptedException e) {
			e.printStackTrace();
		}
		catch(TwitterException e) {
			e.printStackTrace();
		}
		finally {
			client.stop();
		}
		
	}
	

}
