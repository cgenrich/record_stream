package com.github.record_stream;

public interface StreamAggregator<K> {
	long incr(int key);
	long incr(K key);
	long count(int key, long value);
	long count(K key, long value);

	double sum(int key, double value);
	double sum(K key, double value);

	double average(int key, double value);
	double average(K key, double value);

	long getCount(int key);
	long getCount(K key);
	double getSum(int key);
	double getSum(K key);

	void setCount(int key, double value);
	void setCount(K key, double value);
	void setSum(int key, double value);
	void setSum(K key, double value);
}
