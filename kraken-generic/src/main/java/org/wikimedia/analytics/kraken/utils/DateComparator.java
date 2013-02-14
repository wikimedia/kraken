package org.wikimedia.analytics.kraken.utils;

import java.util.Comparator;
import java.util.Date;

public class DateComparator implements Comparator<Date> {


    //static Comparator<Date> date_comparator = new Comparator<Date>() {

    public final int compare(final Date s1, final Date s2) {
        return s1.compareTo(s2);
    }
}

