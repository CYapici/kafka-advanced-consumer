package unit.com.cagatayyapici.kafka.consumer.client;
import static org.junit.Assert.assertNotNull;

import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.cagatayyapici.kafka.consumer.Consumer;

import io.reactivex.observables.ConnectableObservable;
import io.reactivex.schedulers.Schedulers;
import unit.com.cagatayyapici.kafka.consumer.util.test.Parameters;

/**
 * The class <code>ConsumerTest</code> contains tests for the class
 * 
 * 
 * @author cagatayyapici
 */
public class ConsumerTest extends Parameters {

	/**
	 * Run the Consumer(String,String,String,long) constructor test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testConstructor1() throws Exception {
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		Consumer result = new Consumer(groupID, topics, bootStrapServers, pollTimeOut);

		assertNotNull(result);
	}

	/**
	 * Run the Consumer(Properties,String,String,String,long,long) constructor test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testConstructor2() throws Exception {
		Properties consumerConfig = new Properties();
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		Consumer fixture = new Consumer(consumerConfig, groupID, topics, bootStrapServers, pollTimeOut);

		assertNotNull(fixture);
	}

 
	/**
	 * Run the void subscribeActual(Observer<? super Map<String,String>>) method
	 * test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testSubscribeActual_1() throws Exception {
		Properties consumerConfig = new Properties();
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		Consumer consumer = new Consumer(consumerConfig, groupID, topics, bootStrapServers, pollTimeOut);

		ConnectableObservable<Map<String, String>> observable = consumer.subscribeOn(Schedulers.io()).publish();

		observable.connect();

		observable.subscribe();
	}

	/**
	 * Run the void subscribeActual(Observer<? super Map<String,String>>) method
	 * test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testSubscribeActual_2() throws Exception {
		Properties consumerConfig = new Properties();
		String groupID = GROUPID;
		String topics = TOPIC;
		String bootStrapServers = BOOTSTRAP_URL;
		long pollTimeOut = 1L;

		Consumer result = new Consumer(consumerConfig, groupID, topics, bootStrapServers, pollTimeOut);

		ConnectableObservable<Map<String, String>> observable = result.subscribeOn(Schedulers.io()).publish();

		observable.connect();

		observable.subscribe();

	}

}