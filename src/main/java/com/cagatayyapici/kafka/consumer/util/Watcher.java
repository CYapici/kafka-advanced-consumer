package com.cagatayyapici.kafka.consumer.util;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.subjects.BehaviorSubject;

/**
 * 
 * @author cagatayyapici
 *
 * @param <T>
 */
public class Watcher<T> extends Observable<T> {
	private T val;
	private final BehaviorSubject<T> subj;

	public Watcher() {
		subj = BehaviorSubject.create();
	}

	public Watcher(T value) {
		this();
		this.set(value);
	}

	public T get() {
		return val;
	}

	public void set(T value) {
		this.val = value;
		subj.onNext(value);
	}

	public boolean isNull() {
		return val == null;
	}

	@Override
	protected void subscribeActual(Observer<? super T> observer) {
		subj.subscribe(observer);
	}
}