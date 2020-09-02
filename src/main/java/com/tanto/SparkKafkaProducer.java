package com.tanto;

import java.util.ArrayList;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SparkKafkaProducer {
	public static void main(String[] args) throws Exception {
		//Spark konfiguration
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Kafka Produser");
		
		//spark kontext
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		sparkContext.setLogLevel("ERROR");
		
		//set konfigurastion for kafka producer
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//instansiasi kafka produs
		KafkaProducer<String, String> produs = new KafkaProducer<String, String>(props);
		
		//ambil data untuk di kirim ke kafka produs
		JavaRDD<String> jsonFile = sparkContext.textFile("/home/baskara/eclipse-workspace/Kafka-Spark-Stream/sources/daerah.json");
		
		//mapping data yang sudah didapet
		JavaRDD<String> rddData = jsonFile.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String data) throws Exception {
				// TODO Auto-generated method stub
				//instansiasi object mapper
				ObjectMapper mapper = new ObjectMapper();
				JsonNode node = mapper.readTree(data);
				
				//prepare variable for new data 
				List<String> result = new ArrayList<String>();
				
				//loop the data
				for(JsonNode n:node) {
					result.add(mapper.writeValueAsString(n));
				}
				return result.iterator();
			}
		});
		
		//collect rdd data 
		List<String> dataRecord = rddData.collect();
		//loop data for record 
		for(String d:dataRecord) {
			Thread.sleep(1000);
			//instance new Producer Record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic-java", d);
			//send record
			produs.send(record);
		}
		
		//producer close
		produs.close();
 		
	}

}
