package com.ishansong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class HealthCosumer {
    public static void consumers(){
        Properties properties = new Properties();
        //origin 192.168.10.9:9092,192.168.10.8:9092,192.168.10.15:9092
        //bigdata-dev-mq:9092,bigdata-dev-mq:9093,bigdata-dev-mq:9094
        properties.put("bootstrap.servers", "bigdata-dev-mq:9092,bigdata-dev-mq:9093,bigdata-dev-mq:9094");
        properties.put("group.id", "group-1");
        properties.put("client.id","id.demo");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");//earliest,latest,none
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        //courier_health_check_req 请求
        //courier_health_check_resp 返回
        //courier_health_check_req_exception

        kafkaConsumer.subscribe(Arrays.asList("courier_health_check_resp"));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("time=%s,topic = %s,value = %s", record.timestamp(),record.topic(),record.value());
                System.out.println();
            }

        }
    }

    public static void main(String[] args) {
        HealthCosumer.consumers();
    }
}
