package com.cagatayyapici.kafka.consumer.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.cagatayyapici.kafka.consumer.util.RecordBuilderType;
import com.cagatayyapici.kafka.consumer.util.Watcher;

/**
 * Scheduled Listener
 * 
 * @author cagatayyapici
 *
 */
public class ScheduledProcessor implements Runnable {
	private ExecutorService executorService;

	private Map<String, String> resultMap;
	private Map<Integer, List<ConsumerRecord<String, String>>> activityCache;
	private RecordConsumerBuilderImpl recordConsumerr;
	private AtomicBoolean isBusy = new AtomicBoolean(false);
	private AtomicBoolean isMultipleTopicProcessor = new AtomicBoolean(false);
	private Watcher<Map<String, String>> watcherForMap;
	private RecordBuilderType builderType;

	public AtomicBoolean getIsBusy() {
		return isBusy;
	}

	public ScheduledProcessor(RecordConsumerBuilderImpl rescordConsumer,
			Map<Integer, List<ConsumerRecord<String, String>>> activityCache, ExecutorService executorService,
			Map<String, String> resultMap, RecordBuilderType builderType, boolean isMultipleTopicProcessor) {
		this.isMultipleTopicProcessor.set(isMultipleTopicProcessor);
		this.builderType = builderType;
		this.activityCache = activityCache;
		this.executorService = executorService;
		this.resultMap = resultMap;
		this.recordConsumerr = rescordConsumer;
		this.watcherForMap = builderType != RecordBuilderType.ABSTRACTMAP ? null
				: (Watcher<Map<String, String>>) recordConsumerr.getWatcher();
	}

	public void run() {

		isBusy.set(true);

		List<Future<Boolean>> futureList = new ArrayList<>();

		Set<Integer> allPartitions = activityCache.keySet();

		for (Integer key : allPartitions) {
			List<ConsumerRecord<String, String>> activityMap = activityCache.get(key);
			Future<Boolean> finished = executorService.submit(new RecordProcessor(recordConsumerr.getWatcher(),
					activityMap, resultMap, builderType, isMultipleTopicProcessor.get()));
			futureList.add(finished);
		}

		while (futureList.stream().filter(s -> !s.isDone()).count() != 0)
			;

		activityCache.clear();

		isBusy.set(false);

		if (resultMap.size() > 0) {

			if (builderType == RecordBuilderType.MANUAL) {
				recordConsumerr.setHasData(new HashMap<String, String>(resultMap));

			} else {
				if (builderType == RecordBuilderType.ABSTRACTMAP)
					watcherForMap.set(new HashMap<String, String>(resultMap));

			}
			recordConsumerr.clear();
		}

	}

}
