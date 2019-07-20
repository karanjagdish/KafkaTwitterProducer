package bd.karanjag.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerCallback implements Callback{
	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		// TODO Auto-generated method stub
		if(exception == null) {
			System.out.printf("\nTweet with offset %d has been written to topic %s\n", metadata.offset(),metadata.topic());
		}
		else {
			System.out.println(exception.getMessage());
		}
	}
	
}
