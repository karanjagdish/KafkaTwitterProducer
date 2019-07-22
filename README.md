<h1>Simple Scalable Kafka Pipeline for Twitter</h1>

<h3>Twitter API</h3>

This program uses both the Hose Bird Stream API and the Twitter4J library.

The [HBC API](https://github.com/twitter/hbc) was used to get real time tweets in the raw JSON and the [Twitter4J](http://twitter4j.org/en/) library was used to convert the JSON into java object used the Status class of the library.

**To Run this code** : You need your own Consumer and API keys which you can get by following [these steps](https://auth0.com/docs/connections/social/twitter)

<h3>Kafka Cluster Implemented</h3> 

The architecture used was a **single node multi broker** set up. 
Apache Zookeeper was installed and configured instead of using the zookeeeper script that comes with the kafka installation. The 3 Kafka brokers set up were running on the same virtual machine. 

For more details on the architecture and configuration I found [this article](https://dzone.com/articles/kafka-setup) on how to setup Kafka very useful.

<h3>Kafka Producer and Consumer</h3>

The producer and consumer classes implemented are thread safe as they act individual and there is no inter process communication. 
The classes are _currently atleast_ written in away that a single thread of each can run fine (i.e. One Producer thread and One Consumer thread).

The producer writes the string reply from the client to kafka (Extract stage) and the consumer consumes this string and parses it into the status object of the Twitter4J library allowing for easier transforms (Transform Stage).

This project forms a good base for an ETL pipeline involving twitter streams.

<h4>Warning</h4>

While running the code it was found that some status texts from the HBC client are incomplete and end with a link to the actual tweet instead displaying the entire tweets text. If anyone finds a solution please add it in the issues section.

