package com.cagatayyapici.kafka.consumer.builder;

import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens for rebalance of topics
 * 
 * @author cagatayyapici
 *
 */
public class RebalanceListener implements ConsumerRebalanceListener {

	private static final Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

	public void onPartitionsAssigned(Collection<TopicPartition> arg0) {
		arg0.forEach(topicPartition -> {
			logger.warn(topicPartition.topic());
		});
	}

	public void onPartitionsRevoked(Collection<TopicPartition> arg0) {
		arg0.forEach(topicPartition -> {
			logger.warn(topicPartition.topic());
		});
	}
}
