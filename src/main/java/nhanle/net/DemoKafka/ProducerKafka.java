package nhanle.net.DemoKafka;

import java.time.LocalDateTime;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerKafka {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println( "Apache Kafka Producer" );
        String kafkaServer = "192.168.0.9:9092";
        String topicName = "my-hello-world-topic";
        
        
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Tao Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //for (int i = 1; i <= 10; i++) {
        try {
        while(true) {
            String key = "Key-" + LocalDateTime.now().toString();
            String value = "Hello Kafka " + LocalDateTime.now().toString();

            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            // gui tin nhan bat dong bo
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Sent message: (" + key + ", " + value + ") at offset " + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
            try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        }finally {
	        // Dong producer
	        producer.close();
        }
	}

}
