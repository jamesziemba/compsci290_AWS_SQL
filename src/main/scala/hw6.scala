import org.apache.spark.rdd.RDD
import scala.io.Source
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType,StructField,StringType};
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

import org.apache.spark.streaming._

import org.apache.spark.SparkConf

import org.apache.spark.sql.types._


object hw6 {
	
	
	def main(args: Array[String]) {
		if (args.length < 2) {
     	 System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
     	 System.exit(1)
    	}
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("hw6")
            //.set("spark.eventLog.enabled","true")
            //.set("spark.eventLog.dir",root_dir)
 
        val sc = new SparkContext(conf)

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		 val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
        .map(_._2)
    messages.foreachRDD {rdd =>
    /* this check is here to handle the empty collection error
       after the 3 items in the static sample data set are processed */
      if (rdd.toLocalIterator.nonEmpty) {
        val df = sqlContext.read.json(rdd)
            
        df.registerTempTable("logs")

        val numApps = sqlContext.sql("SELECT Event FROM logs WHERE Event = 'SparkListenerApplicationStart'")
            .collect()


        println("HERE IS THE ANSWER for numApps:\n\n\n\n\n\n")
        println(numApps.length)

        val numEvents = sqlContext.sql("SELECT DISTINCT Event FROM logs")
            .collect()

        println("HERE IS THE ANSWER for numEvents:\n\n\n\n\n\n")
        println(numEvents.length)

        val numEventsNotFinished = sqlContext.sql("SELECT Event FROM logs WHERE Event='SparkListenerTaskStart'")
            .collect()
        val numEventsFinished = sqlContext.sql("SELECT Event FROM logs WHERE Event='SparkListenerTaskEnd'")
            .collect()


        println("HERE IS THE ANSWER for numEventsNotFinished:\n\n\n\n\n\n")
        println(numEventsNotFinished.length-numEventsFinished.length)
        println("\n\n\n\n\n\nHERE IS THE END:\n\n\n\n\n\n")

        val averageRuntime = sqlContext.sql("SELECT AVG(`Task Metrics`.`Executor Run Time`) FROM logs WHERE `Task Metrics`.`Executor Run Time` > 0")
        println("HERE IS THE ANSWER for averageRuntime:\n\n\n\n\n\n")

        averageRuntime.foreach(println)    
        
        println("\n\n\n\n\n\nHERE IS THE END:\n\n\n\n\n\n")

        val x = averageRuntime.collect()
        println("\n\n\n\n\nXXXXXX")

        val y = x(0)(0)
        println(y)
        println("\n\n\n\n\n")

        val runTimes = sqlContext.sql(s"""SELECT `Stage ID`,`Task Info`.`Executor ID`,`Task Info`.`Task ID`,`Task Metrics`.`Executor Run Time` FROM logs WHERE `Task Metrics`.`Executor Run Time` > $y""")
        println("\n\n\n\n\n")

        runTimes.foreach(println)    

        println("Sql Number 5\n\n\n\n\n")
          
    
		}
    }
}
}
