package unit.com.cagatayyapici.kafka.consumer.processor.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.IdentityHashMap;
import java.util.LinkedList;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import com.cagatayyapici.kafka.consumer.builder.RecordProcessor;
import com.cagatayyapici.kafka.consumer.util.RecordBuilderType;
import com.cagatayyapici.kafka.consumer.util.Watcher;

public class RecordProcessorTest {
	/**
	 * Run the
	 * RecordProcessor(List<ConsumerRecord<String,String>>,Map<String,String>)
	 * constructor test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testRecordProcessor() throws Exception {

		RecordProcessor result = new RecordProcessor(new Watcher<ConsumerRecord<String, String>>(),
				new LinkedList<ConsumerRecord<String, String>>(), new IdentityHashMap<String, String>(),
				RecordBuilderType.ABSTRACTITERATOR, false);

		// add additional test code here
		assertNotNull(result);
		assertEquals(Boolean.TRUE, result.call());
	}

	/**
	 * Run the Boolean call() method test.
	 *
	 * @throws Exception
	 *
	 * 
	 */
	@Test
	public void testCall() throws Exception {
		RecordProcessor fixture = new RecordProcessor(new Watcher<ConsumerRecord<String, String>>(),
				new LinkedList<ConsumerRecord<String, String>>(), new IdentityHashMap<String, String>(),
				RecordBuilderType.ABSTRACTITERATOR, false);

		Boolean result = fixture.call();

		// add additional test code here
		assertNotNull(result);
		assertEquals("true", result.toString());
		assertEquals(true, result.booleanValue());
	}

}
