package com.roi.consumer;  

import java.io.BufferedReader; 
import java.io.File; 
import java.io.FileReader; 
import java.io.IOException; 
import java.util.Properties; 
import java.util.Collections; 

import org.apache.kafka.clients.consumer.KafkaConsumer; 
import org.apache.kafka.clients.consumer.Consumer; 
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;  
import org.apache.kafka.clients.consumer.ConsumerRecords;  

public class ConsumerMain {  
    public static void main(String[] args) { 
		System.out.println("start");
        Properties props = new Properties(); 
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);  
		consumer.subscribe(Collections.singletonList("test"));

		while (true) {
			final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
			final int giveUp = 100;   
			int noRecordsCount = 0;

			if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

			consumerRecords.forEach(record -> { 
				System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
				});
				consumer.commitAsync();
		}
		consumer.close();
		System.out.println("Done");
	}
}

/*
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
 
import java.util.Collections;
import java.util.Properties;

public class ConsumerMain {
    public static void main(String[] args) {
        Consumer consumerThread = new Consumer("test");
        consumerThread.start();
    }

	public class Consumer extends ShutdownableThread {
		private final KafkaConsumer<Integer, String> consumer;
		private final String topic;
		
		public static final String KAFKA_SERVER_URL = "localhost";
		public static final int KAFKA_SERVER_PORT = 9092;
		public static final String CLIENT_ID = "ConsumerMain";
	
		public Consumer(String topic) {
			super("ConsumerMain", false);
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
			props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
			props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
			props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	
			consumer = new KafkaConsumer<>(props);
			this.topic = topic;
		}
	
		@Override
		public void doWork() {
			consumer.subscribe(Collections.singletonList(this.topic));
			ConsumerRecords<Integer, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
			}
		}
	
		@Override
		public String name() {
			return null;
		}
	
		@Override
		public boolean isInterruptible() {
			return false;
		}
	}
}

/*
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

public class ConsumerMain {
	public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "ipaddress:6667");
        props.put("bootstrap.servers", "localhost:9092"); 
		props.put("kafka.topic"      , "test");
		props.put("key.deserializer"   , "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("group.id"          , "my-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
*/
