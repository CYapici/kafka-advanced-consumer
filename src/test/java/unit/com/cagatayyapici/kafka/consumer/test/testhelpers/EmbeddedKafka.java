package unit.com.cagatayyapici.kafka.consumer.test.testhelpers;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

 
public abstract class EmbeddedKafka {
	public static final KafkaCluster embeddedKafka = new KafkaCluster(1);
	public static String BOOTSTRAP_URL;
	public static final String TOPIC = "test_topic";
	public static final String GROUP_ID = "KafkaSampleConsumer";

	@BeforeClass
	public static void setUP() {
		embeddedKafka.start();
		BOOTSTRAP_URL = embeddedKafka.bootstrapServers();
	}

	@AfterClass
	public static void tearDown() {
		embeddedKafka.stop();
	}

	protected Properties consumerProps() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_URL);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return props;
	}

	protected Properties producerProps() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.bootstrapServers());
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 1);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		return props;
	}
	
	protected Properties consumerProps(String groupId) {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.bootstrapServers());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		return props;
	}

	protected Properties producerProps(String groupId) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.bootstrapServers());
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return props;
	}

}
