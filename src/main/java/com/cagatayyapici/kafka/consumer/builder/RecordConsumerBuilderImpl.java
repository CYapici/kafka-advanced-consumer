package com.cagatayyapici.kafka.consumer.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cagatayyapici.kafka.consumer.util.Params;
import com.cagatayyapici.kafka.consumer.util.RecordBuilderType;
import com.cagatayyapici.kafka.consumer.util.RecordConsumerBuilderImplConfig;
import com.cagatayyapici.kafka.consumer.util.Validator;
import com.cagatayyapici.kafka.consumer.util.Watcher;

import io.reactivex.Observer;

/**
 * 
 * Builder of Consumer
 * 
 * @author cagatayyapici
 *
 */

public class RecordConsumerBuilderImpl implements Runnable, RecordConsumerBuilder {

	private static final Logger logger = LoggerFactory.getLogger(RecordConsumerBuilderImpl.class);
	private Properties config;
	private AtomicBoolean closed = new AtomicBoolean(false);
	public AtomicBoolean iterateToNext = new AtomicBoolean(false);
	public AtomicBoolean terminate = new AtomicBoolean(false);
	private long pollTimeOut;
	private KafkaConsumer<String, String> consumer;
	private List<String> allTopics;
	private ExecutorService executorService;
	private ScheduledExecutorService executor;
	private ScheduledProcessor scheduledProcessor;
	private Map<String, String> resultMap;
	private Map<TopicPartition, OffsetAndMetadata> commitMap;
	private Map<Integer, List<ConsumerRecord<String, String>>> activityCache;
	private Observer<? super Map<String, String>> observer;
	private Object watcher;

	public Object getWatcher() {
		return watcher;
	}

	public void setWatcher(Observer<? super Map<String, String>> observer) {
		this.observer = observer;
	}

	public void setHasData(Map<String, String> data) {

		observer.onNext(data);

	}

	public Properties getConfig() {
		return config;
	}

	public RecordConsumerBuilderImpl(String groupID, String topics, String bootStrapServers, long pollTimeOut,
			RecordBuilderType type) {
		this(RecordConsumerBuilderImplConfig.getConsumerConfig(null, groupID, bootStrapServers), groupID, topics,
				bootStrapServers, pollTimeOut, type);

	}

	public RecordConsumerBuilderImpl(Properties consumerConfig, String groupID, String topics, String bootStrapServers,
			long pollTimeOut, RecordBuilderType type) {
		// validation
		Validator.validateArgs(groupID, topics, pollTimeOut);
		// thread per topic
		allTopics = Arrays.asList(topics.split(","));
		final int topicSize = allTopics.size();

		if (type != RecordBuilderType.MANUAL) {
			watcher = type == RecordBuilderType.ABSTRACTMAP ? new Watcher<Map<String, String>>()
					: new Watcher<ConsumerRecord<String, String>>();
		}

		this.pollTimeOut = pollTimeOut;
		this.resultMap = new ConcurrentHashMap<>();
		this.activityCache = new ConcurrentHashMap<>();

		this.config = RecordConsumerBuilderImplConfig.getConsumerConfig(consumerConfig, groupID, bootStrapServers);
		this.consumer = new KafkaConsumer<String, String>(config);

		this.executorService = Executors.newFixedThreadPool(topicSize);
		this.executor = Executors.newScheduledThreadPool(1);
		this.commitMap = new HashMap<>();

		try {
			initScheduler(type, topicSize);
		} catch (InterruptedException e) {
			logger.error("Error while initializing the scheduler: {}", e.getMessage());
		}

	}

	public Map<String, String> getResultMap() {
		return resultMap;
	}

	@Override
	public void run() {
		consumer.subscribe(allTopics, new RebalanceListener());

		while (!closed.get()) {
			consume();
			if (terminate.get())
				shutDown(1);
		}

	}

	@Override
	public void consume() {
		Set<TopicPartition> topicPartitions = consumer.assignment();

		while (scheduledProcessor.getIsBusy().get()) {
			try {
				consumer.pause(topicPartitions);
				Thread.sleep(100);
				consumer.resume(topicPartitions);
				consumer.poll(0);
			} catch (InterruptedException e) {
				logger.error("Error while consuming: {}", e.getMessage());
			}
		}

		if (activityCache.isEmpty())
			this.commit();

		ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
		Set<TopicPartition> partitions = records.partitions();
		for (TopicPartition part : partitions) {
			Integer partitionId = part.partition();
			List<ConsumerRecord<String, String>> partRecords = records.records(part);
			List<ConsumerRecord<String, String>> curActivities = activityCache.getOrDefault(partitionId,
					new ArrayList<ConsumerRecord<String, String>>());
			curActivities.addAll(partRecords);
			activityCache.put(partitionId, curActivities);
			commitMap.put(part, new OffsetAndMetadata(partRecords.get(partRecords.size() - 1).offset() + 1));
		}
	}

	@Override
	public void initScheduler(RecordBuilderType type, int topicSize) throws InterruptedException {

		scheduledProcessor = new ScheduledProcessor(this, activityCache, executorService, resultMap, type,
				topicSize > 1);

		executor.scheduleAtFixedRate(scheduledProcessor, Params.DELAYFORSECONDS, Params.WAITFORSECONDS,
				TimeUnit.SECONDS);

	}

	@Override
	public void commit() {
		consumer.commitSync(commitMap);
		commitMap.clear();
	}

	protected void shutDown(long timeoutInSecs) {

		closeConsumer();
		executorService.shutdown();

		try {
			executorService.awaitTermination(timeoutInSecs, TimeUnit.SECONDS);

		} catch (InterruptedException e) {
			logger.error("Error while shutting down: {}", e.getMessage());
		} finally {
			if (!executorService.isTerminated()) {
				logger.warn("Exit for termination.");
			}

			executorService.shutdownNow();
			logger.info("Shutting down Executor.");
		}
	}

	@Override
	public void clear() {
		resultMap.clear();
	}

	@Override
	public void terminate(long timeOut) {
		terminate.set(true);
		logger.info("Will be terminating gracefully...");

	}

	private void closeConsumer() {
		try {
			closed.set(true);
			consumer.wakeup();
		} catch (Exception e) {
			logger.error("Error on closing : {}", e.getMessage());
		} finally {
			consumer.close();
		}
	}

	// for System.gc();
	@Override
	protected void finalize() {
		terminate(1);
	}

}
