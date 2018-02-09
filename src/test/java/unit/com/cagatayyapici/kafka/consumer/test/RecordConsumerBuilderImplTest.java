package unit.com.cagatayyapici.kafka.consumer.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.cagatayyapici.kafka.consumer.builder.RecordConsumerBuilderImpl;
import com.cagatayyapici.kafka.consumer.util.RecordBuilderType;

import unit.com.cagatayyapici.kafka.consumer.util.test.Parameters;

/**
 * The class <code>RecordConsumerBuilderImplTest</code> contains tests for the
 * class <code>{@link RecordConsumerBuilderImpl}</code>.
 *
 * 
 * @author cagatayyapici
 */
public class RecordConsumerBuilderImplTest extends Parameters {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	/**
	 * Run the RecordConsumerBuilderImpl(String,String,String,long) constructor
	 * test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testRecordConsumerBuilderImpl() throws Exception {
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		RecordConsumerBuilderImpl result = new RecordConsumerBuilderImpl(groupID, topics, bootStrapServers, pollTimeOut,
				RecordBuilderType.ABSTRACTITERATOR);

		assertNotNull(result);
	}

	/**
	 * Run the RecordConsumerBuilderImpl(Properties,String,String,String,long)
	 * constructor test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testRecordConsumerBuilderImpl_2() throws Exception {
		Properties consumerConfig = new Properties();
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		RecordConsumerBuilderImpl result = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics,
				bootStrapServers, pollTimeOut, RecordBuilderType.ABSTRACTITERATOR);

		assertNotNull(result);
	}

	/**
	 * Run the void Clear() method test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testClear() throws Exception {
		Properties consumerConfig = new Properties();
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		RecordConsumerBuilderImpl fixture = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics,
				bootStrapServers, pollTimeOut, RecordBuilderType.ABSTRACTITERATOR);
		fixture.getResultMap().put("key", "val");

		fixture.clear();

		assertTrue(fixture.getResultMap().size() == 0);

	}

//	/**
//	 * Run the void Fork() method test.
//	 *
//	 * @throws Exception
//	 *
//	 * 
//	 */
//	@Test
//	public void testyieldToNext() throws Exception {
//
//		Properties consumerConfig = new Properties();
//		String groupID = GROUPID;
//		String topics = TOPIC;
//		String bootStrapServers = BOOTSTRAP_URL;
//		long pollTimeOut = 1L;
//
//		RecordConsumerBuilderImpl fixture = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics,
//				bootStrapServers, pollTimeOut, RecordBuilderType.ABSTRACTITERATOR);
//
//		assertTrue(!fixture.getHasData());
//
//		fixture.setHasData();
//
//		assertTrue(fixture.getHasData());
//
//	}

//	/**
//	 * Run the Boolean IsCompleted() method test.
//	 *
//	 * @throws Exception
//	 *
//	 * 
//	 */
//	@Test
//	public void testYieldToNext() throws Exception {
//		Properties consumerConfig = new Properties();
//		String groupID = GROUPID;
//		String topics = TOPIC;
//		String bootStrapServers = BOOTSTRAP_URL;
//		long pollTimeOut = 1L;
//
//		RecordConsumerBuilderImpl fixture = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics,
//				bootStrapServers, pollTimeOut, RecordBuilderType.ABSTRACTITERATOR);
//
//		Boolean result = fixture.getHasData();
//
//		assertNotNull(result);
//	}

	/**
	 * Run the void consume() method test.
	 *
	 * @throws Exception
	 *
	 */

	@Test(expected = IllegalStateException.class)
	public void testConsume() {
		Properties consumerConfig = new Properties();
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		RecordConsumerBuilderImpl fixture = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics,
				bootStrapServers, pollTimeOut, RecordBuilderType.ABSTRACTITERATOR);

		fixture.consume();
	}

	// java.lang.IllegalStateException: Consumer is not subscribed to any topics or
	// assigned any partitions

	/**
	 * Run the Properties getConfig() method test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testGetConfig() throws Exception {
		Properties consumerConfig = new Properties();
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		RecordConsumerBuilderImpl fixture = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics,
				bootStrapServers, pollTimeOut, RecordBuilderType.ABSTRACTITERATOR);

		Properties result = fixture.getConfig();

		assertNotNull(result);
	}

	/**
	 * Run the Map<String, String> getResultMap() method test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testGetResultMap() throws Exception {
		Properties consumerConfig = new Properties();
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		RecordConsumerBuilderImpl fixture = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics,
				bootStrapServers, pollTimeOut, RecordBuilderType.ABSTRACTITERATOR);

		Map<String, String> result = fixture.getResultMap();

		assertNotNull(result);
	}

	/**
	 * Run the void initScheduler() method test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testInitScheduler() throws Exception {
		Properties consumerConfig = new Properties();
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		RecordConsumerBuilderImpl fixture = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics,
				bootStrapServers, pollTimeOut, RecordBuilderType.ABSTRACTITERATOR);
		fixture.initScheduler(RecordBuilderType.ABSTRACTITERATOR, 1);

	}

}
