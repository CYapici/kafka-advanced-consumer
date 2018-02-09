package com.cagatayyapici.kafka.consumer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.cagatayyapici.kafka.consumer.builder.RecordConsumerBuilderImpl;
import com.cagatayyapici.kafka.consumer.util.RecordBuilderType;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.observables.ConnectableObservable;
import io.reactivex.schedulers.Schedulers;

/**
 * listens for callbacks continuously
 * 
 * @author cagatayyapici
 *
 */
public class Consumer extends Observable<Map<String, String>> {

	private RecordConsumerBuilderImpl consumer = null;
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private ConnectableObservable<Map<String, String>> activeObservable = this.subscribeOn(Schedulers.io()).publish();
	Observer<? super Map<String, String>> observer;

	public ConnectableObservable<Map<String, String>> getActiveObservable() {
		return activeObservable;
	}

	public Consumer(String groupID, String topics, String bootStrapServers, long pollTimeOut) {

		consumer = new RecordConsumerBuilderImpl(groupID, topics, bootStrapServers, pollTimeOut,
				RecordBuilderType.MANUAL);

		executor.submit(consumer);

		activeObservable = this.subscribeOn(Schedulers.io()).publish();

		activeObservable.connect();

	}

	public Consumer(Properties consumerConfig, String groupID, String topics, String bootStrapServers,
			long pollTimeOut) {

		consumer = new RecordConsumerBuilderImpl(consumerConfig, groupID, topics, bootStrapServers, pollTimeOut,
				RecordBuilderType.MANUAL);

		executor.submit(consumer);

		activeObservable = this.subscribeOn(Schedulers.io()).publish();

		activeObservable.connect();
	}

	@Override
	protected void subscribeActual(Observer<? super Map<String, String>> observer) {
		consumer.setWatcher(observer);
	}

	public void dispose() {
		consumer.terminate(10);
	}
}
