package nhanle.net.DemoKafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerKafka {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Apache Kafka Consummer");
		String kafkaServer = "192.168.0.9:9092";
		String topicName = "my-hello-world-topic";

		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, topicName);
		properties.put("enable.auto.commit", "false");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		
		consumer.subscribe(Collections.singletonList(topicName));
		
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<String, String> record : records) {
					System.out.printf("Consummer: Key: %s, Value: %s, Offset: %d, Partition: %d \r\n.", record.key(), record.value(), record.offset(), record.partition());
				}
				consumer.commitSync();
			}
		} catch (Exception e) {
			System.out.println("Loi: " + e.getMessage());
		} finally {
            consumer.close(); // Đóng consumer khi kết thúc
        }
		
	}	
}
