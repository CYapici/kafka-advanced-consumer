package unit.com.cagatayyapici.kafka.consumer.processor.test;

import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.LinkedList;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import com.cagatayyapici.kafka.consumer.builder.RebalanceListener;

public class RebalanceListenerTest {
	/**
	 * Run the RebalanceListener() constructor test.
	 *
	 *  
	 */
	@Test
	public void testRebalanceListener () throws Exception {
		RebalanceListener result = new RebalanceListener();
		assertNotNull(result);
		// add additional test code here
	}

	/**
	 * Run the void onPartitionsAssigned(Collection<TopicPartition>) method test.
	 *
	 * @throws Exception
	 *
	 *  
	 */
	@Test
	public void testOnPartitionsAssigned () throws Exception {
		RebalanceListener fixture = new RebalanceListener();
		Collection<TopicPartition> arg0 = new LinkedList();

		fixture.onPartitionsAssigned(arg0);

		// add additional test code here
	}

	/**
	 * Run the void onPartitionsRevoked(Collection<TopicPartition>) method test.
	 *
	 * @throws Exception
	 *
	 *  
	 */
	@Test
	public void testOnPartitionsRevoked () throws Exception {
		RebalanceListener fixture = new RebalanceListener();
		Collection<TopicPartition> arg0 = new LinkedList();

		fixture.onPartitionsRevoked(arg0);

		// add additional test code here
	}

}
