package com.tanto;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tanto.model.MyModel;

public class SparkKafkaProducerCSV {
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark Kafka");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");
		
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> produs = new KafkaProducer<String, String>(props);
		
		JavaRDD<String> csvData = sc.textFile("/home/baskara/eclipse-workspace/Kafka-Spark-Stream/sources/exampledata.csv");
		
		JavaRDD<String> rddData = csvData.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String dataCsv) throws Exception {
				// TODO Auto-generated method stub
				ObjectMapper mapper = new ObjectMapper();
				List<String> dataRow = Arrays.asList(dataCsv.split("\n"));
				
				List<String> dataJson = new ArrayList<String>();
				for(String row:dataRow) {
					String[] data = row.split(",");
					
					MyModel model = new MyModel(data[0], data[0], data[1], Integer.valueOf(data[2]), data[3],
							Integer.valueOf(data[4]), Integer.valueOf(data[5]), Integer.valueOf(data[6]), data[7],
							data[8], Integer.valueOf(data[9]), data[10], data[11]);
					
					dataJson.add(mapper.writeValueAsString(model));
					
				}
				
				return dataJson.iterator();
			}
		});
		
		List<String> data = rddData.collect();
		for(String d:data) {
			Thread.sleep(1000);
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic-java", d);
			produs.send(record);
		}
		
		produs.close();
	}

}
