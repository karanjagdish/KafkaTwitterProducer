package bd.karanjag.kafka.kafka_twitter_producer;

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
        client.run();
    }
}
