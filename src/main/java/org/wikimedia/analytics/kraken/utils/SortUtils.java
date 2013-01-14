package org.wikimedia.analytics.kraken.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

public class SortUtils  {
	public static final Comparator<Date> date_comparator = new DateComparator();

	public static <T extends Comparable<? super T>> List<T> asSortedList(
			Collection<T> coll) {
		List<T> list = new ArrayList<T>(coll);

		java.util.Collections.sort(list);
		return list;
	}
}
