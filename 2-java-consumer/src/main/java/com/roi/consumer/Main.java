package com.roi.consumer;  

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;
import com.google.gson.Gson;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

class KafkaMessage {
	private String id;
	private String timestamp;
    private String data;
    
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}	
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}
}

public class Main {
	public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "ipaddress:6667");
        props.put("bootstrap.servers", "localhost:9092"); 
		props.put("kafka.topic"      , "test");
		props.put("key.deserializer"   , "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("group.id"          , "my-group");
		// properties.put("compression.type" , "gzip");
		// properties.put("max.partition.fetch.bytes", "2097152");
		// properties.put("max.poll.records"          , "500");
		
		runMainLoop(args, props);
	}
	
	static void runMainLoop(String[] args, Properties properties) throws InterruptedException, UnsupportedEncodingException {
		// Create Kafka producer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		try {
      	    consumer.subscribe(Arrays.asList(properties.getProperty("kafka.topic")));
      	    System.out.println("Subscribed to topic " + properties.getProperty("kafka.topic"));	
  	      	
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %s, offset = %d, key = %s, value = %s\n", record.partition(), record.offset(), record.key(), decodeMsg(record.value()).getData() );
                }
      	    }
		}
        finally {
            consumer.close();
        }
    }
	
	public static KafkaMessage decodeMsg(String json) throws UnsupportedEncodingException {
        Gson gson = new Gson();
        KafkaMessage msg = gson.fromJson(json, KafkaMessage.class); 
        byte[] encodedData = Base64.getDecoder().decode(msg.getData()); 
		msg.setData(new String(encodedData, "utf-8"));
	    return msg;		
	}
}
