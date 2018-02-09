package com.cagatayyapici.kafka.consumer.util;

import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 
 * @author cagatayyapici
 *
 */
public class RecordConsumerBuilderImplConfig {
	public static final String ACKS_ALL = "all";
	public static final String ENABLE_IDEMPOTENCE_TRUE = "true";
	public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_ONE = "1";
	public static final String LINGER_MS_ZERO = "0";
	public static final String BATCH_SIZE_VALUE = "1048576";
	public static final String BATCH_COUNT_VALUE = "1000";
	public static final String BATCH_TIME_SPAN_VALUE = "100";

	public static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

	public static Properties getDefaultConfig(String GROUP_ID, String BOOTSTRAP_SERVERS) {

		final Properties producerConfig = new Properties();

		/**
		 * Serializer class for key that implements the
		 * org.apache.kafka.common.serialization.Serializer interface.
		 */
		producerConfig.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		/**
		 * Serializer class for value that implements the
		 * org.apache.kafka.common.serialization.Serializer interface.
		 */
		producerConfig.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		/**
		 * When set to 'true', the producer will ensure that exactly one copy of each
		 * message is written in the stream. If 'false', producer retries due to broker
		 * failures, etc., may write duplicates of the retried message in the stream.
		 * Note that enabling idempotence requires max.in.flight.requests.per.connection
		 * to be less than or equal to 5, retries to be greater than 0 and acks must be
		 * 'all'. If these values are not explicitly set by the user, suitable values
		 * will be chosen. If incompatible values are set, a ConfigException will be
		 * thrown.
		 */
		producerConfig.put(ENABLE_IDEMPOTENCE_CONFIG, ENABLE_IDEMPOTENCE_TRUE);

		/**
		 * The number of acknowledgments the producer requires the leader to have
		 * received before considering a request complete. This controls the durability
		 * of records that are sent. The following settings are allowed:
		 */
		producerConfig.put(ProducerConfig.ACKS_CONFIG, ACKS_ALL);

		producerConfig.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_ONE);

		producerConfig.put(LINGER_MS_CONFIG, LINGER_MS_ZERO);

		producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE_VALUE);

		producerConfig.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

		producerConfig.put("key.deserializer", KEY_DESERIALIZER);

		producerConfig.put("value.deserializer", VALUE_DESERIALIZER);

		producerConfig.put("group.id", GROUP_ID);

		producerConfig.put("bootstrap.servers", BOOTSTRAP_SERVERS);

		return producerConfig;
	}

	public static Properties getConsumerConfig(Properties customConfig, String GROUP_ID, String bootStrapServers) {
		Properties config = getDefaultConfig(GROUP_ID, bootStrapServers);
		if (customConfig != null)
			config.putAll(customConfig);

		return config;
	}

}
