package com.roi.producer;  

import java.io.BufferedReader; 
import java.io.File; 
import java.io.FileReader; 
import java.io.IOException; 
import java.util.Properties; 

import org.apache.kafka.clients.producer.KafkaProducer; 
import org.apache.kafka.clients.producer.Producer; 
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;  

public class ProducerMain {  
    public static void main(String[] args) { 
        Properties props = new Properties(); 
        // props.put("bootstrap.servers", "localhost:9092"); 
        // props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
        // props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
        // props.put("acks", "all");  

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");  

    // props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
    // props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    // props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    // props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        Producer<String, String> producer = new KafkaProducer<>(props);  
        try { 
            File file = new File(ProducerMain.class.getClassLoader().getResource("transactions.txt").getFile()); 
            BufferedReader br = new BufferedReader(new FileReader(file)); 
            String line; 
            while ((line = br.readLine()) != null) { 
                String[] lineArray = line.split(":"); 
                String key = lineArray[0]; 
                String value = lineArray[1]; 
                ProducerRecord<String, String> msg = new ProducerRecord<>("test", key, value);
                producer.send(msg); 
            } 
            br.close(); 
        } catch (IOException e) { 
            throw new RuntimeException(e); 
        }  
        producer.close(); 
    }  
}



