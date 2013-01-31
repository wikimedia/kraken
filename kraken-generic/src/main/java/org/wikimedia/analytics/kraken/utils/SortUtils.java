package org.wikimedia.analytics.kraken.utils;

import java.util.*;

public class SortUtils  {
	public static final Comparator<Date> date_comparator = new DateComparator();

	public static <T extends Comparable<? super T>> List<T> asSortedList(Collection<T> coll) {
		List<T> list = new ArrayList<T>(coll);

		Collections.sort(list);
		return list;
	}
}
