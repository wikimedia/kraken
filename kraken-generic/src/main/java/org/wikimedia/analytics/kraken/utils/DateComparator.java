package org.wikimedia.analytics.kraken.utils;

import java.util.Comparator;
import java.util.Date;

public class DateComparator implements Comparator<Date> {


	//static Comparator<Date> date_comparator = new Comparator<Date>() {

	public final int compare(Date s1, Date s2){
		return s1.compareTo(s2);
	}
}

//	public int compare(Date o1, Date o2) {
//		return s1.compareTo(s2);
//	}

