package com.cagatayyapici.kafka.consumer.builder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.cagatayyapici.kafka.consumer.util.RecordBuilderType;
import com.cagatayyapici.kafka.consumer.util.Watcher;

/**
 * Processes each record Currently is capable of merging different topics
 * 
 * @author cagatayyapici
 *
 */
public class RecordProcessor implements Callable<Boolean> {
	private List<ConsumerRecord<String, String>> records;
	private Map<String, String> resultMap;
	private boolean isMultipleTopicProcessor;
	private Watcher<ConsumerRecord<String, String>> observableRecord;
	private boolean isMapProcessor;

	public RecordProcessor(Object observable, List<ConsumerRecord<String, String>> records,
			Map<String, String> summaryMap, RecordBuilderType builderType, boolean isMultipleTopicProcessor) {

		this.isMapProcessor = builderType == RecordBuilderType.ABSTRACTMAP || builderType == RecordBuilderType.MANUAL;
		this.observableRecord = isMapProcessor ? null : (Watcher<ConsumerRecord<String, String>>) observable;
		this.records = records;
		this.resultMap = summaryMap;
		this.isMultipleTopicProcessor = isMultipleTopicProcessor;
	}

	@Override
	public Boolean call() throws Exception {
		records.stream().forEach(p -> {
			if (!isMapProcessor) {
				eachrecord(p);
				resultMap.put(p.value(), p.value());
			} else {
				String key = isMultipleTopicProcessor ? p.topic() + "-" + p.key() : p.key();
				resultMap.put(key, p.value());
			}

		});
		return true;
	}

	public void eachrecord(ConsumerRecord<String, String> p) {
		observableRecord.set(p);
	}

}
