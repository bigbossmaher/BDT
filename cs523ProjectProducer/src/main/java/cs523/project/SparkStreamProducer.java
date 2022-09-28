
package cs523.project;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import kafka.server.KafkaConfig;

import com.twitter.hbc.core.Client;

import scala.Tuple2;
import twitter4j.GetTweetsKt;
import twitter4j.Status;
import twitter4j.TweetsResponse;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.UsersExKt;
import twitter4j.UsersResponse;
import twitter4j.V2DefaultFields;

public class SparkStreamProducer {

	
	public static void main(String[] args)throws IOException, TwitterException, InterruptedException  {

		Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR);
		Client client;
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(25);
		/** Setting up a connection */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hbEndpoint = new StatusesFilterEndpoint();
		// Term that I want to search on Twitter
		hbEndpoint.trackTerms(Lists.newArrayList("ukraine"));
		// Twitter API and tokens
		Authentication hosebirdAuth = new OAuth1("1R5FaVapm53Gnc0OgijvMmd28",
				"xlggPOkv9C61OcEW9JznROomkKPqPwZxBJAg25DsKwnUT7506y",
				"154526910-OBcHAEnnraYFLBj5YEZlw2KtoUULzROIBnkcorVG",
				"dLnUIaAE2t7jAlzTYNY3dkaSGBIMMtBi3kCbWcUNj6SEZ");

		/** Creating a client */
		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client")
				.hosts(hosebirdHosts).authentication(hosebirdAuth)
				.endpoint(hbEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hbClient = builder.build();

		client =  hbClient;
		client.connect();
		
		/* kafak configuration */
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer kafkaProducer = new KafkaProducer(properties);
		
		
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				//logger.info(msg);
				JSONObject js = new JSONObject(msg);
				//System.out.println(msg);
				//logger.info(js.toString());
				JSONArray hasTags = js.getJSONObject("entities").getJSONArray("hashtags");
				JSONArray array = new JSONArray(hasTags);  
				for(int i=0; i < array.length(); i++)   
				{  
				JSONObject object = array.getJSONObject(i);  
				System.out.println(object.getString("text"));  
				kafkaProducer.send(new ProducerRecord("twitter-events", object.getString("text")));
				}  
				
				
				

			}
		}


		
		
		

		
		

	}

}