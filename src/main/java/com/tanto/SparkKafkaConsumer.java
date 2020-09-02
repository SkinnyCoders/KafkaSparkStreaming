package com.tanto;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class SparkKafkaConsumer {
	public static final String group = "belajar";
	public static final String kafkaTopic = "topic-java";
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("Consumer");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.OFF);
		
		//konfigurasi kafka consumer
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put("group.id", group);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    
	    Collection<String> topic = Arrays.asList(kafkaTopic.split(","));
	    
	    JavaInputDStream<ConsumerRecord<String, String>> streamRecord = KafkaUtils
	    		.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topic, props));
	    
	    JavaDStream<String> recordRow = streamRecord.map(new Function<ConsumerRecord<String,String>, String>() {

			@Override
			public String call(ConsumerRecord<String, String> row) throws Exception {
				// TODO Auto-generated method stub
				return row.value();
			}
		});
	    
	    recordRow.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			
			@Override
			public void call(JavaRDD<String> data) throws Exception {
				// TODO Auto-generated method stub
				data.foreach(new VoidFunction<String>() {
					
					@Override
					public void call(String recordValue) throws Exception {
						// TODO Auto-generated method stub
						System.out.println(recordValue);
					}
				});
			}
		});
	    
	    recordRow.print();
	    jssc.start();
	    jssc.awaitTermination();
		
	}

}
