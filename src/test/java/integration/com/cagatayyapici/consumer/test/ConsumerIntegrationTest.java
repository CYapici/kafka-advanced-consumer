package integration.com.cagatayyapici.consumer.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cagatayyapici.kafka.consumer.Consumer;

import unit.com.cagatayyapici.kafka.consumer.test.testhelpers.EmbeddedKafka;

/**
 * checking and confirming sent value
 * 
 * @author cagatayyapici
 *
 */
public class ConsumerIntegrationTest extends EmbeddedKafka {

	private static final String[] TOPICS1 = new String[] { "test_topic1" };
	private static final Logger logger = LoggerFactory.getLogger(ConsumerIntegrationTest.class);
	private static final Pair<String, String> record = new ImmutablePair<String, String>("testKey", "testValue");
	private static List<ConcurrentHashMap<String, String>> currentProcessed;

	@Test
	public void consumerCall() throws InterruptedException, ExecutionException {
		currentProcessed = Collections.synchronizedList(new ArrayList<ConcurrentHashMap<String, String>>());

		final long timeoutMin = 2, pollIntervalNanoSec = 251;
		final Properties producerProperties = producerProps(GROUP_ID);
		final Producer<String, String> producer = new KafkaProducer<String, String>(producerProperties);
		final String MERGED_TOPIC = String.join(",", TOPICS1);

		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(TOPICS1[0], record.getKey(),
				record.getValue());

		Future<RecordMetadata> recordMetadataFuture = producer.send(producerRecord);

		recordMetadataFuture.get();

		producer.close();

		Consumer listener = new Consumer(consumerProps(GROUP_ID), GROUP_ID, MERGED_TOPIC, BOOTSTRAP_URL, 100);

		listener.getActiveObservable().subscribe(item -> onKafkaGet(item));

		Awaitility.await().atMost(timeoutMin, TimeUnit.MINUTES).pollInterval(pollIntervalNanoSec, TimeUnit.NANOSECONDS)
				.until(() -> {
					return currentProcessed.stream().filter(p -> p.containsValue(record.getValue()))
							.collect(Collectors.toList()).size() > 0;
				});

		assertEquals(1, currentProcessed.stream().filter(p -> p.containsValue(record.getValue()))
				.collect(Collectors.toList()).size());
	}

	public static void onKafkaGet(Map<String, String> records) {
		currentProcessed.add((new ConcurrentHashMap<>(records)));
		logger.debug("invoked with", records.toString());
	}

}
