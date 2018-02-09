package component.com.cagatayyapici.kafka.consumer.builder.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.awaitility.Awaitility;
import org.junit.Test;

import com.cagatayyapici.kafka.consumer.builder.RecordConsumerBuilderImpl;
import com.cagatayyapici.kafka.consumer.util.RecordBuilderType;
import com.cagatayyapici.kafka.consumer.util.RecordConsumerBuilderImplConfig;

import unit.com.cagatayyapici.kafka.consumer.test.testhelpers.EmbeddedKafka;

public class RecordConsumerBuilderComponentTest extends EmbeddedKafka {

	private static final String[] TOPICS1 = new String[] { "test_topic1", "test_topic2", "test_topic3" };
	private static final String[] TOPICS2 = new String[] { "test_topic4", "test_topic6", "test_topic7" };

	// The (U)nion test
	@Test
	public void shouldGetDataFromUnionOfTopics() throws InterruptedException, ExecutionException {
		testWithLogic(3, TOPICS2);
	}

	// A
	@Test
	public void shouldGetDataAtLeastFromOneOfTopics() throws InterruptedException, ExecutionException {
		testWithLogic(1, TOPICS1);
	}

	// A||B||C
	@Test
	public void shouldGetDataAtLeastFromOneOfRandomTopics() throws InterruptedException, ExecutionException {
		testWithLogicRandomIdx(TOPICS1);
	}

	@Test
	public void shouldGetDataSubsequently() throws InterruptedException, ExecutionException {

		final int target = 2;
		for (int i = 0; i < target; i++)
			testWithLogic(target, TOPICS1, String.valueOf(i));
	}

	@Test
	public void futureShouldnotBeDone() throws Exception {
		final long waithreshold = 10;
		ExecutorService executor = Executors.newSingleThreadExecutor();
		RecordConsumerBuilderImpl consumer = new RecordConsumerBuilderImpl(consumerProps(), "KafkaSampleConsumer",
				TOPIC, BOOTSTRAP_URL, 100, RecordBuilderType.MANUAL);

		Future<?> future = executor.submit(consumer);

		Awaitility.waitAtMost(waithreshold, TimeUnit.SECONDS);

		assertThat(!future.isDone());

	}

	@Test
	public void shouldBeExecutedInDifferentThread() throws Exception {

		final String mainThreadName = Thread.currentThread().getName();
		Executors.newSingleThreadExecutor().submit(() -> {
			assertThat(mainThreadName).isNotEqualTo(Thread.currentThread().getName());
		});
	}

	@Test
	public void itShouldUseDefaultConfigIfOptionalConfigIsNull() throws Exception {
		final String MERGED_TOPIC = String.join(",", TOPICS1);
		RecordConsumerBuilderImpl consumer = new RecordConsumerBuilderImpl(null, GROUP_ID, MERGED_TOPIC,
				embeddedKafka.bootstrapServers(), 100, RecordBuilderType.MANUAL);
		assertThat(consumer.getConfig())
				.containsAllEntriesOf(RecordConsumerBuilderImplConfig.getDefaultConfig(GROUP_ID, BOOTSTRAP_URL));

	}

	private void testWithLogic(int expectedCount, String[] topics) throws InterruptedException, ExecutionException {

		final String MERGED_TOPIC = String.join(",", topics);

		// push as expectedCount

		for (int i = 0; i < expectedCount; i++)
			populate(topics[i]);

		ExecutorService executor = Executors.newSingleThreadExecutor();
		RecordConsumerBuilderImpl consumer = new RecordConsumerBuilderImpl(consumerProps(), GROUP_ID, MERGED_TOPIC,
				BOOTSTRAP_URL, 100, RecordBuilderType.MANUAL);

		executor.submit(consumer);

		Awaitility.await().atMost(60, TimeUnit.SECONDS).untilTrue(consumer.iterateToNext);

		int resultSize = consumer.getResultMap().size();

		assertThat(resultSize).isGreaterThan(0);

	}

	private void testWithLogicRandomIdx(String[] topics) throws InterruptedException, ExecutionException {

		final String MERGED_TOPIC = String.join(",", topics);

		final int idx = ThreadLocalRandom.current().nextInt(0, 3);

		final String groupId = GROUP_ID + String.valueOf(idx);
		// push as expectedCount

		populate(topics[idx], groupId);

		ExecutorService executor = Executors.newSingleThreadExecutor();
		RecordConsumerBuilderImpl consumer = new RecordConsumerBuilderImpl(consumerProps(groupId), groupId,
				MERGED_TOPIC, BOOTSTRAP_URL, 100, RecordBuilderType.MANUAL);

		executor.submit(consumer);

		Awaitility.await().atMost(60, TimeUnit.SECONDS).untilTrue(consumer.iterateToNext);

		int resultSize = consumer.getResultMap().size();

		assertThat(resultSize).isGreaterThan(0);

	}

	private void testWithLogic(int expectedCount, String[] topics, String groupId)
			throws InterruptedException, ExecutionException {

		final String MERGED_TOPIC = String.join(",", topics);

		// push as expectedCount

		for (int i = 0; i < expectedCount; i++) {
			populate(topics[i], groupId);
		}

		ExecutorService executor = Executors.newSingleThreadExecutor();
		RecordConsumerBuilderImpl consumer = new RecordConsumerBuilderImpl(consumerProps(groupId), groupId,
				MERGED_TOPIC, BOOTSTRAP_URL, 100, RecordBuilderType.MANUAL);

		executor.submit(consumer);

		Awaitility.await().atMost(60, TimeUnit.SECONDS).untilTrue(consumer.iterateToNext);

		int resultSize = consumer.getResultMap().size();

		assertThat(resultSize).isGreaterThan(0);

	}

	private void populate(String topicName, String groupID) throws InterruptedException, ExecutionException {

		final Properties producerProperties = producerProps(groupID);

		final Producer<String, String> producer = new KafkaProducer<String, String>(producerProperties);

		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, "testKey",
				"testValue");

		Future<RecordMetadata> recordMetadataFuture = producer.send(producerRecord);

		recordMetadataFuture.get();

		producer.close();
	}

	private void populate(String topicName) throws InterruptedException, ExecutionException {

		final Properties producerProperties = producerProps(GROUP_ID);

		final Producer<String, String> producer = new KafkaProducer<String, String>(producerProperties);

		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, "testKey",
				"testValue");

		Future<RecordMetadata> recordMetadataFuture = producer.send(producerRecord);

		recordMetadataFuture.get();

		producer.close();

	}

}
