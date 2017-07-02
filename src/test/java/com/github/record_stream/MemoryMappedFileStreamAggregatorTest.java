package com.github.record_stream;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MemoryMappedFileStreamAggregatorTest {
	private MemoryMappedFileStreamAggregator aggregator;

	@Test
	public void daysAverage() {
		for (int day = 1; day < 32; day++) {
			double avg = 0;
			for (int i = day + 3; i > 0; i--) {
				avg = aggregator.average(day, i * 100 + i);
			}
			int n = day + 3, sum = n * (n + 1) / 2;
			assertEquals("failed on day: " + day, (sum * 100 + sum) / n, avg, 0.999);
		}
		for (int day = 1; day < 32; day++) {
			int n = day + 3;
			n = n * (n + 1) / 2;
			assertEquals("failed on day: " + day, n * 100 + n, aggregator.getSum(day), 0);
			assertEquals("failed on day: " + day, day + 3, aggregator.getCount(day), 0);
		}
	}

	@Test
	public void daysCount() {
		for (int day = 1; day < 32; day++) {
			for (int i = day + 3; i > 0; i--) {
				aggregator.count(day, i * 100 + i);
			}
		}
		for (int day = 1; day < 32; day++) {
			int n = day + 3;
			n = n * (n + 1) / 2;
			assertEquals("failed on day: " + day, n * 100 + n, aggregator.getCount(day), 0);
		}
	}

	@Test
	public void daysIncr() {
		for (int i = 1; i < 100; i++) {
			assertEquals(i, aggregator.incr(0));
		}
	}

	@Test
	public void daysSum() {
		for (int day = 1; day < 32; day++) {
			for (double i = day + 3; i > 0; i--) {
				aggregator.sum(day, i * 100 + i + i / 100);
			}
		}
		for (int day = 1; day < 32; day++) {
			double n = day + 3;
			n = n * (n + 1) / 2;
			assertEquals("failed on day: " + day, n * 100 + n + n / 100, aggregator.getSum(day), .01);
		}
	}

	@Test
	public void sumAndCount() {
		sumAndCountRange(1, 31);
	}

	public void sumAndCountRange(int min, int max) {
		for (int day = max; day >= min; day--) {
			aggregator.setCount(day, day);
			aggregator.setSum(day, day);
		}
		aggregator.setCount("offset", 100);
		aggregator.setSum("offset", 100);
		aggregator.setCount("one", 101);
		aggregator.setSum("one", 101);
		aggregator.setCount("two", 102);
		aggregator.setSum("two", 102);
		aggregator.setCount("three", 103);
		aggregator.setSum("three", 103);

		for (long day = max; day >= min; day--) {
			assertEquals(day, aggregator.getCount((int) day), 0);
			assertEquals(day, aggregator.getSum((int) day), 0);
		}
		assertEquals(100, aggregator.getCount("offset"), 0);
		assertEquals(100, aggregator.getSum("offset"), 0);
		assertEquals(101, aggregator.getCount("one"), 0);
		assertEquals(101, aggregator.getSum("one"), 0);
		assertEquals(102, aggregator.getCount("two"), 0);
		assertEquals(102, aggregator.getSum("two"), 0);
		assertEquals(103, aggregator.getCount("three"), 0);
		assertEquals(103, aggregator.getSum("three"), 0);
	}

	@Test
	public void shortOffsetRangeTest() throws Exception {
		tearDown();
		aggregator = new MemoryMappedFileStreamAggregator(25, 31, "target/monthBackup", "offset", "one", "two", "three");
		sumAndCountRange(25, 31);
	}

	@Test
	public void readExistingFile() throws IOException {
		aggregator.incr(1);
		aggregator.sum(1, 10);
		MemoryMappedFileStreamAggregator next = new MemoryMappedFileStreamAggregator(1, 31, "target/monthBackup", "offset", "one", "two", "three");
		assertEquals(1, next.getCount(1));
		assertEquals(10, next.getSum(1), 0);
		next.close();
	}

	@Before
	public void setUp() throws Exception {
		aggregator = new MemoryMappedFileStreamAggregator(1, 31, "target/monthBackup", "offset", "one", "two", "three");
	}

	@After
	public void tearDown() throws Exception {
		aggregator.delete();
	}

}
