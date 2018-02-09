package unit.com.cagatayyapici.kafka.consumer.test.testhelpers;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.cagatayyapici.kafka.consumer.ConsumerMap;

import kafka.consumer.Consumer;
import unit.com.cagatayyapici.kafka.consumer.util.test.Parameters;

public class ValidatorTest extends ConsumerMap {
	public static final String BOOTSTRAP_URL = "localhost:9092";
	public static final String TOPIC = "test_topic";
	public static final String GROUPID = "1";
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void itShouldThrowExceptionIfgroupIDIsEmpty() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Parameter Group Id cannot be null or empty");
		Consume("", TOPIC, BOOTSTRAP_URL, 1000);
	}

	@Test
	public void itShouldThrowExceptionIftopicsIsEmpty() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Parameter Topic List  cannot be null or empty");
		Consume(GROUPID, "", BOOTSTRAP_URL, 1000);

	}

	@Test
	public void itShouldThrowExceptionIfpollTimeoutIsLessThanZero() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Parameter Poll Timeout can not be less than zero.");
		Consume(GROUPID, TOPIC, BOOTSTRAP_URL, -1);
	}

}
