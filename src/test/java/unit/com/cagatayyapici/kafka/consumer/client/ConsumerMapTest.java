package unit.com.cagatayyapici.kafka.consumer.client;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.cagatayyapici.kafka.consumer.ConsumerMap;

import unit.com.cagatayyapici.kafka.consumer.test.testhelpers.ValidatorTest;
import unit.com.cagatayyapici.kafka.consumer.util.test.Parameters;

/**
 * The class <code>ConsumerAllTest</code> contains tests for the class
 * 
 *
 * 
 * @author cagatayyapici
 * 
 */
public class ConsumerMapTest extends Parameters {
	/**
	 * Run the void Consume(String,String,String,long) method test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testConsume1() throws Exception {
		ConsumerMap fixture = new ValidatorTest();
		fixture.Consume(GROUPID, TOPIC, BOOTSTRAP_URL, POLLTIMEOUT);
	}

	/**
	 * Run the void Consume(Properties,String,String,String,long) method test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testConsume2() throws Exception {
		ConsumerMap fixture = new ValidatorTest();
		Properties consumerConfig = new Properties();

		fixture.Consume(consumerConfig, GROUPID, TOPIC, BOOTSTRAP_URL, POLLTIMEOUT);

	}

	/**
	 * Run the void dispose() method test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testDispose() throws Exception {
		ConsumerMap fixture = new ValidatorTest();
		fixture.Consume(GROUPID, TOPIC, BOOTSTRAP_URL, POLLTIMEOUT);
		fixture.dispose();

	}

	/**
	 * Run the void onRecord(Map<String,String>) method test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testOnRecord() throws Exception {
		ConsumerMap fixture = new ValidatorTest();
		Map<String, String> records = new IdentityHashMap();
		fixture.onRecord(records);

	}

}