package com.edc.kafka;

import com.edc.utils.ConfigProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Mtime on 2017/8/26.
 */
public class KafkaPublishClient {
    private static Producer<String, String> producer = new KafkaProducer<String, String>(getKafkaProperties());
    private KafkaPublishClient(){}

    private static Properties getKafkaProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", ConfigProperties.get("bootstrap.servers"));
        props.put("acks", ConfigProperties.get("acks","all"));
        props.put("retries", ConfigProperties.get("retries","0"));
        props.put("batch.size", ConfigProperties.get("batch.size","16384"));
        props.put("linger.ms", ConfigProperties.get("linger.ms","1"));
        props.put("key.serializer", ConfigProperties.get("key.serializer","org.apache.kafka.common.serialization.StringSerializer"));
        props.put("value.serializer", ConfigProperties.get("value.serializer","org.apache.kafka.common.serialization.StringSerializer"));
        return props;
    }

    public static void send(String topic,String msg){
        producer.send(new ProducerRecord<String, String>(topic, msg));
    }

    public static void main(String[] args){
        String topic = "customer_base_group";
        for(int i = 0; i < 5; i++){
            String msg = "200"+i+"~select count(1) from tableA where clumn1='123'~2000~0.1~memberNo~asc";
//            KafkaPublishClient.send(topic, msg);
        }

        Properties props = new Properties();
//        props.put("bootstrap.servers", "192.168.52.160:9092,192.168.52.161:9092,192.168.52.162:9092");
        props.put("bootstrap.servers", "bi101:9092,bi102:9092,bi103:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0; i < 10; i++){
            String msg = "200"+i+"~select count(1) from tableA where clumn1='123'~2000~0.1~memberNo~asc";
            producer.send(new ProducerRecord<String, String>(topic, msg));
        }

    }

}
