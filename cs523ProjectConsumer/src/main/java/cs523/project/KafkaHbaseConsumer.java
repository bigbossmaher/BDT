

package cs523.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaHbaseConsumer {

    public static void main(String[] args) throws IOException {
        
    	/* Hbase configuration */
        Configuration config = HBaseConfiguration.create();
	    HTable hTable = new HTable(config, "twitter");
	    
		/* Kafka configuration */
    	Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");
        
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        
        topics.add("twitter-events");
        kafkaConsumer.subscribe(topics);
        
        try{
            while (true){
            	ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            	
            	for (ConsumerRecord<String, String> record : records){                    
            		  System.out.printf("%s\n", record.value());
                                		  
            			/* save into HBase */
                	String hashtag = record.value();
					String tag =  record.value();
					String count =  "1";
					
                    Put  p = new Put(Bytes.toBytes(record.offset()+""));
         	       p.add(Bytes.toBytes("tweet"),Bytes.toBytes("tag"),Bytes.toBytes(tag));	       
         	       p.add(Bytes.toBytes("tweet"), Bytes.toBytes("count"),Bytes.toBytes(count));   
         	       hTable.put(p);
         	       
            	}
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
            hTable.close();
        }
    }
}