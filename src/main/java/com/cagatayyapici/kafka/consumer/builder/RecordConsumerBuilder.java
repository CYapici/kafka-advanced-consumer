package com.cagatayyapici.kafka.consumer.builder;

import com.cagatayyapici.kafka.consumer.util.RecordBuilderType;

/**
 * 
 * @author cagatayyapici
 *
 */
public interface RecordConsumerBuilder {

	void consume();

	void initScheduler(RecordBuilderType type, int topicSize) throws InterruptedException;

	void commit();

	void clear();

	void terminate(long timeOut);

}
