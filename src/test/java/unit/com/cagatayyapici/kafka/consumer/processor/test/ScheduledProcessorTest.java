package unit.com.cagatayyapici.kafka.consumer.processor.test;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import com.cagatayyapici.kafka.consumer.builder.RecordConsumerBuilderImpl;
import com.cagatayyapici.kafka.consumer.builder.ScheduledProcessor;
import com.cagatayyapici.kafka.consumer.util.RecordBuilderType;

import unit.com.cagatayyapici.kafka.consumer.util.test.Parameters;

public class ScheduledProcessorTest extends Parameters {

	ScheduledProcessor processor;

	@Before
	public void setUp() throws Exception {
		processor = PowerMockito.mock(ScheduledProcessor.class);
		whenNew(ScheduledProcessor.class).withAnyArguments().thenReturn(processor);
	}

	/**
	 * Check the runnning thread by id
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void itShouldStartInDifferentThread() {
		Thread thread = new Thread(processor);
		thread.start();
		assertNotEquals(1, thread.getId());
	}

	/**
	 * Run the
	 * ScheduledProcessor(RecordConsumerBuilderImpl,Map<Integer,List<ConsumerRecord<String,String>>>,ExecutorService,Map<String,String>)
	 * constructor test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testScheduledProcessor() throws Exception {
		Properties consumerConfig = new Properties();
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		RecordConsumerBuilderImpl fixture = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics,
				bootStrapServers, pollTimeOut, RecordBuilderType.ABSTRACTITERATOR);
		Map<Integer, List<ConsumerRecord<String, String>>> activityCache = new IdentityHashMap();
		ExecutorService executorService = new ForkJoinPool();
		Map<String, String> resultMap = new IdentityHashMap();

		ScheduledProcessor result = new ScheduledProcessor(fixture, activityCache, executorService, resultMap,
				RecordBuilderType.ABSTRACTITERATOR, false);

		assertNotNull(result);
	}

	/**
	 * Run the AtomicBoolean IsBusy() method test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testIsBusy() throws Exception {

		Properties consumerConfig = new Properties();
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		RecordConsumerBuilderImpl recordConsumer = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics,
				bootStrapServers, pollTimeOut, RecordBuilderType.ABSTRACTITERATOR);

		ScheduledProcessor fixture = new ScheduledProcessor(recordConsumer, new IdentityHashMap(), new ForkJoinPool(),
				new IdentityHashMap(), RecordBuilderType.ABSTRACTITERATOR, false);

		AtomicBoolean result = fixture.getIsBusy();

		assertNotNull(result);
	}

	/**
	 * Run the void run() method test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testRun_1() throws Exception {
		ScheduledProcessor fixture = new ScheduledProcessor(
				new RecordConsumerBuilderImpl("", "", "", 1L, RecordBuilderType.ABSTRACTITERATOR),
				new IdentityHashMap(), new ForkJoinPool(), new IdentityHashMap(), RecordBuilderType.ABSTRACTITERATOR,
				false);

		fixture.run();

	}

}
