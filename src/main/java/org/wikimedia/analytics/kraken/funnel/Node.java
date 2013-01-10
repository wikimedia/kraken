package org.wikimedia.analytics.kraken.funnel;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

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

public class Node {
	/** The keys. */
	public List<String> keys = new ArrayList<String>();
	
	/** The visited. */
	public List<Date> visited;
	public String url;

	/**
	 * Instantiates a new node. 
	 *
	 */
	public Node() {
		keys.add("client");
		keys.add("languageCode");
		keys.add("project");
		keys.add("namespace");
		keys.add("page");
		keys.add("event");
	}

	/**
	 * Compare two nodes and determine whether they can be considered the same.
	 *
	 * @param b the b
	 * @return true, if successful
	 */
	public boolean equals(Node b) {
		if (this == b) return true;
		if (!(b instanceof Node)) return false;
		Node node = (Node)b;
		if (this.toString().equals(node.toString())) {
			return true;
		} else {
			return false;
		}
	}
}
