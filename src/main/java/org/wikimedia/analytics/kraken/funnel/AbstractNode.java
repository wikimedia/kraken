package org.wikimedia.analytics.kraken.funnel;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

public abstract class AbstractNode {
	/**
	 * Component	Description					Example
	 * client		client application 			web, iphone, android
	 * languagecode	project language			en, fr, ja
	 * project		project name				wikipedia, wikisource
	 * namespace								0, 2
	 * page			page or functional grouping		
	 * event		action taken by user		impression, click
	 *
	 * suggested encoding of an event:
	 * client:languagecode:project:namespace:page:event
	 * 
	 * Inspiration taken from "The Uniﬁed Logging Infrastructure
	 * for Data Analytics at Twitter" 
	 * (http://vldb.org/pvldb/vol5/p1771_georgelee_vldb2012.pdf)
	 */

	/** The keys. */
	public List<String> keys = new ArrayList<String>();

	/** The visited. */
	public List<Date> visited;

	/** The useragent. */
	public String useragent;

	/** The wikis. */
	public Pattern wikis = Pattern.compile("");

	/**
	 * Split project variable from funnel in language and project component.
	 *
	 * @param fproject the new project
	 */
	abstract void setProject(String fproject);

	/**
	 * Compare two nodes and determine whether they can be considered the same.
	 *
	 * @param b the b
	 * @return true, if successful
	 */
	public abstract boolean equals(Node b);

	public abstract boolean equals(FunnelNode b);
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public abstract String toString();
}
