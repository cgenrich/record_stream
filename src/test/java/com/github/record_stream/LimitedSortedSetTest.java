package com.github.record_stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LimitedSortedSetTest {

	private LimitedSortedSet<Integer> set;

	@Test
	public void lessThanMax() {
		assertTrue(set.add(1));
		assertFalse(set.isEmpty());
		assertEquals(1, set.iterator().next(), 0);
	}

	@Test
	public void atMax() {
		for (int i = 0; i < 5; i++) {
			assertTrue(set.add(i));
			assertEquals(i, set.last(), 0);
			assertEquals(0, set.first(), 0);
		}
		assertFalse(set.isEmpty());
		assertEquals(5, set.size());
	}

	@Test
	public void greaterThanMax() {
		for (int i = 0; i < 6; i++) {
			assertTrue(set.add(i));
			assertEquals(i, set.last(), 0);
		}
		assertFalse(set.isEmpty());
		assertEquals(5, set.size());
		assertEquals(1, set.first(), 0);
		assertEquals(5, set.last(), 0);
	}

	@Before
	public void setUp() throws Exception {
		set = new LimitedSortedSet<>(5);
	}

	@After
	public void tearDown() throws Exception {
		set.clear();
	}

}
