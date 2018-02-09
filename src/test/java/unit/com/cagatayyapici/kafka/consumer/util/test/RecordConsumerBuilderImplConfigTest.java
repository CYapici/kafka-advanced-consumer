package unit.com.cagatayyapici.kafka.consumer.util.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.cagatayyapici.kafka.consumer.util.RecordConsumerBuilderImplConfig;

public class RecordConsumerBuilderImplConfigTest {

	private static final String defaultGroupId = "1";

	@Test
	public void itShouldReturnDefaultConsumerConfig() {
		Properties consumerConfig = RecordConsumerBuilderImplConfig.getConsumerConfig(null, defaultGroupId,
				"localhost:9092");
		assertThat(consumerConfig).isNotNull();
		assertThat(consumerConfig).containsAllEntriesOf(configEntries(defaultGroupId));
	}

	private Map<String, Object> configEntries(String GROUP_ID) {
		Map<String, Object> map = new HashMap<>();
		map.put("acks", "all");
		map.put("batch.size", "1048576");
		map.put("enable.idempotence", "true");
		map.put("linger.ms", "0");
		map.put("max.in.flight.requests.per.connection", "1");
		map.put("retries", 2147483647);
		map.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		map.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		map.put("group.id", GROUP_ID);

		return map;
	}

}
