package com.github.record_stream;

import java.util.Collection;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

public class LimitedSortedSet<E> extends TreeSet<E> {
	private static final long serialVersionUID = -2444351250877731605L;
	private int maxSize;

	public LimitedSortedSet(int maxSize) {
		this.maxSize = maxSize;
	}

	public LimitedSortedSet(int maxSize, Comparator<? super E> comparator) {
		super(comparator);
		this.maxSize = maxSize;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		boolean added = true;
		for (E i : c) {
			added &= add(i);
		}
		return added;
	}

	@Override
	public boolean add(E o) {
		SortedSet<E> headSet;
		if (size() >= maxSize && !(headSet = headSet(o)).isEmpty()) {
			remove(headSet.first());
		}
		return super.add(o);
	}
}
