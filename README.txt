Please find my code for this assignment, named "hw6.scala" as well as the build.sbt file I used to compile
it in this directory. 

To create the DataFrames, I found a method on the scala documentation website that converts a json input
into a dataframe. From there, I used a pre-generated events log to confirm the functionality of all queries
in part 1 before I added it to my final code. For part 2, As you can see in the beginning of my code, I 
was able to create a streaming context with 2 second batch intervals and then create a Kafka stream with
brokers and topics. My variable "messages" is what ultimately reads in the information written to the
datastream so it can be proccessd by the SQL queries. Each "message" is read in as an RDD, and I use it to
create a df with this line of code:

val df = sqlContext.read.json(rdd)

To learn how to use Kafka, I used the following resources:

https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/DirectKafkaWordCount.scala

This GitHub page greatly helped with starting the direct Kafka stream. I used a lot of code from this page.

bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port topic1,topic2

The command above, which I got from that page, is what ultimately lets me to connect to the Kafka brokers
and start the 2 second batch intervals whereby data can be read in.

http://kafka.apache.org/documentation.html#introduction

This is a link to the Kafka tutorial through which I was able to start the Kafka servers, create topics,
start consumers, and ultimately import and export data. Below are commands I used throughout this process.

//Start zookeeper and server
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

//Create a topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

//Start consumer
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning

//This is an example to show that data can be dynamically written and streamed to a Kafka server
echo -e "foo\nbar" > test.txt

On this assignment, I worked extensively with Nick Pagliuca, Drew Goldstein, Andrew Gauthier, and Steve
Mazzari. We worked together to learn how to use Kafka and to devise the SQL queries.