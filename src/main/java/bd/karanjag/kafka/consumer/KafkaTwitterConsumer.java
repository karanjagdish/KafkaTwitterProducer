package bd.karanjag.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import bd.karanjag.kafka.config.KafkaConfiguration;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

public class KafkaTwitterConsumer implements Runnable{
	private Properties props = new Properties();
	//private KafkaConsumer <Long,String> consumer;
	
	public KafkaTwitterConsumer(){
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaConfiguration.SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "testgroup");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "tweetConsumer1");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		
		//consumer = new KafkaConsumer<>(props);
	}
	
	public void run() {
		
		try(KafkaConsumer<Long, String>consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Arrays.asList(KafkaConfiguration.TOPIC));
			int i = 0;
			while(true) {
				
				//System.out.println("Consumer polling");
				ConsumerRecords<Long,String> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<Long, String> record : records) {
					Status status = TwitterObjectFactory.createStatus(record.value());
					System.out.printf("\n##################\n\nConsumed Tweet Offset = %d\nValue = %s\n", record.offset(),status.getText());
					i++;
				}
				if(i>10)
					break;
				
			}
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}

}
