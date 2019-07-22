package bd.karanjag.kafka.kafka_twitter_producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import bd.karanjag.kafka.consumer.KafkaTwitterConsumer;
import bd.karanjag.kafka.producer.TwitterClient;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        TwitterClient client = new TwitterClient();
        KafkaTwitterConsumer consumer = new KafkaTwitterConsumer();
        
        ExecutorService executorService = Executors.newCachedThreadPool();
        
        executorService.execute(client);
        executorService.execute(consumer);
        executorService.shutdown();
        try {
        	executorService.awaitTermination(2, TimeUnit.MINUTES);
        }
        catch(InterruptedException e) {
        	e.printStackTrace();
        }
        
        System.out.println("Produced and Consumed 10 tweets");
    }
}
