package com.cagatayyapici.kafka.consumer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.cagatayyapici.kafka.consumer.builder.RecordConsumerBuilderImpl;
import com.cagatayyapici.kafka.consumer.util.RecordBuilderType;
import com.cagatayyapici.kafka.consumer.util.Watcher;

/**
 * listens for callbacks gets the result one by one
 * 
 * @author cagatayyapici
 *
 */
public abstract class ConsumerMap {

	private RecordConsumerBuilderImpl consumer = null;
	private final ExecutorService executor = Executors.newSingleThreadExecutor();

	public void Consume(String groupID, String topics, String bootStrapServers, long pollTimeOut) {

		consumer = new RecordConsumerBuilderImpl(groupID, topics, bootStrapServers, pollTimeOut,
				RecordBuilderType.ABSTRACTMAP);

		executor.submit(consumer);

		toWatcher().subscribe(item -> onRecord(item));

	}

	public void Consume(Properties consumerConfig, String groupID, String topics, String bootStrapServers,
			long pollTimeOut) {

		consumer = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics, bootStrapServers, pollTimeOut,
				RecordBuilderType.ABSTRACTMAP);

		executor.submit(consumer);

		toWatcher().subscribe(item -> onRecord(item));
	}

	public void onRecord(Map<String, String> records) {

	}

	private Watcher<Map<String, String>> toWatcher() {
		return (Watcher<Map<String, String>>) consumer.getWatcher();
	}

	public void dispose() {
		consumer.terminate(10);
	}
}
