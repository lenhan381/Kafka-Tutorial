package nhanle.net.DemoKafka;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

public class CreateTopicKafka {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println( "Apache Kafka Create Topic" );
        String kafkaServer = "192.168.0.9:9092";
        String topicName = "my-hello-world-topic";
        
        int partitions = 3; // Số lượng partition
        short replicationFactor = 1; // Hệ số replication (>=1)
        
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        
        try (AdminClient adminClient = AdminClient.create(properties)){
        	NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            
            // Gui yeu cau topic
            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
            
            // Kiem tra ket qua
            result.all().get();
            System.out.println("Topic '" + topicName + "' da tao thanh cong!");
		} catch (Exception e) {
			// TODO: handle exception
			System.err.println("Loi khi tao topic: " + e.getMessage());
		}
	}

}
