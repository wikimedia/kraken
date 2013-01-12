package org.wikimedia.analytics.kraken.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

public class SortUtils implements Comparator<Date> {

	
	static Comparator<Date> date_comparator = new Comparator<Date>() {
		public int compare(Date s1, Date s2){
			return s1.compareTo(s2);
		}
	};
	public SortUtils() {
	}

	public static <T extends Comparable<? super T>> List<T> asSortedList(
			Collection<T> c) {
		List<T> list = new ArrayList<T>(c);

		java.util.Collections.sort(list, date_comparator);
		return list;
	}


	public int compareTo(Date arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int compare(Date o1, Date o2) {
		// TODO Auto-generated method stub
		return 0;
	}
}
