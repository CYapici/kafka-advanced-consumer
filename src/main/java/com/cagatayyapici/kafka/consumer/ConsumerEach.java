package com.cagatayyapici.kafka.consumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.cagatayyapici.kafka.consumer.builder.RecordConsumerBuilderImpl;
import com.cagatayyapici.kafka.consumer.util.RecordBuilderType;
import com.cagatayyapici.kafka.consumer.util.Watcher;

/**
 * listens for callbacks gets the result one by one
 * 
 * @author cagatayyapici
 *
 */
public abstract class ConsumerEach {

	private RecordConsumerBuilderImpl consumer = null;
	private final ExecutorService executor = Executors.newSingleThreadExecutor();

	public void Consume(String groupID, String topics, String bootStrapServers, long pollTimeOut) {

		consumer = new RecordConsumerBuilderImpl(groupID, topics, bootStrapServers, pollTimeOut,
				RecordBuilderType.ABSTRACTITERATOR);

		executor.submit(consumer);

		toWatcher()
				.subscribe(item -> onRecord(item.key(), item.value(), item.topic(), item.partition(), item.offset()));

	}

	public void Consume(Properties consumerConfig, String groupID, String topics, String bootStrapServers,
			long pollTimeOut) {

		consumer = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics, bootStrapServers, pollTimeOut,
				RecordBuilderType.ABSTRACTITERATOR);

		executor.submit(consumer);

		toWatcher()
				.subscribe(item -> onRecord(item.key(), item.value(), item.topic(), item.partition(), item.offset()));
	}

	public void onRecord(String key, String value, String topic, int partition, long offset) {

	}

	private Watcher<ConsumerRecord<String, String>> toWatcher() {
		return (Watcher<ConsumerRecord<String, String>>) consumer.getWatcher();
	}

	public void dispose() {
		consumer.terminate(10);
	}
}
