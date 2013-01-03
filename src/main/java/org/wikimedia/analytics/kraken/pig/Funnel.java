/** Wrapper class for getCountry and getCountryIpV6 */

package org.wikimedia.analytics.kraken.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;

public class Funnel extends EvalFunc<Tuple> {
	private Node funnel;
	private Map<String, Node> urlMap;
	
	public Funnel() {
		
	}

	@Override
	/** Method exec takes a tuple containing four objects:
	 * 1) A bag of tuples containing: the user id, the time stamp, the uri request
	 * 2) A tuple of URLs
	 * 3) An integer as the maximum timeframe between the first and last request in the funnel in seconds
	 * @param input  Tuple containing a bag of the user history and the funnel as a tuple of strings
	 */
	public Tuple exec(Tuple input) throws IOException {
		/*
		 * Doing the sorting in Pig Latin, rather than in your UDF, is important for a couple of reasons. 
		 * One, it means Pig can offload the sorting to MapReduce. MapReduce has the ability to sort data 
		 * by a secondary key while grouping it. So the order statement in this case does not require a 
		 * separate sorting operation. Two, it means that your UDF does not need to wait for all data 
		 * to be available before it starts processing. Instead, it can use the Accumulator interface 
		 * (see the section called “Accumulator Interface”) which is much more memory efficient.
		 */
		DataBag bag = (DataBag) input.get(0);
		Iterator<Tuple> it = bag.iterator();
		Tuple funnelUrls = (Tuple) input.get(1);
		Tuple funnelDag = (Tuple) input.get(2);
		int timeframe = (Integer) input.get(3);
		List<String> history = new ArrayList<String>();
		List<Integer> timestamps = new ArrayList<Integer>();
		Node drop = null;
		
		//Create graph data structure
		if(funnel == null && urlMap == null) {
			funnel = constructFunnel(funnelDag.getAll(), funnelUrls.getAll());
		}
		
		//Create history and timestamp list
		while(it.hasNext()) {
			Tuple t = (Tuple) it.next();
			if(t!=null && t.size() == 3 && t.get(0) !=null) {			
				String url = (String) t.get(2);
				int timestamp = (Integer) t.get(1);
				history.add(url);
				timestamps.add(timestamp);
			}
		}
		
		//Create the output tuple
		Tuple output = TupleFactory.getInstance().newTuple(2);
		
		//Iterate through user's request history
		for(int i = 0; i < history.size(); i++) {
			String url = history.get(i);
			if(!url.equals(funnel.getUrl())) {
				//if the current url is not the root of the funnel continue
				continue;
			}
			Node curr = funnel;
			int beginning = timestamps.get(i);
			int end = beginning;
			for(int j = i+1; j < history.size(); j++) {
				end = timestamps.get(j);
				//break out of the loop when time delta is greater than given timeframe.
				if(end - beginning > timeframe) {
                                        // we can fast-forward the funnel analysis by incrementing i with j
                                        i += j;
					break;
				}
				
				Node candidate = urlMap.get(history.get(j));
				//if child is not in funnel or is not a child of the current node then break out of loop
				if(candidate == null || !curr.getChildren().contains(candidate)) {
					drop = curr;
                                        // we can fast-forward the funnel analysis by incrementing i with j
                                        i += j;
					break;
				}
				//if candidate node is a leaf then user has went through the funnel and return true
				if(candidate.getChildren().size() == 0) {
					output.set(0, true);
					output.set(1, null);
					return output;
				}
				//set current to candidate if candidate is a child of current node.
				curr = candidate;
			}
		}

		output.set(0, false);
		String dropUrl = (drop == null) ? null : drop.getUrl();
		output.set(1, dropUrl);
		return output;
		
	}
	
	private Node constructFunnel(List<Object> edges, List<Object> urls) throws ExecException {
		List<Node> nodes = new ArrayList<Node>();
		urlMap = new HashMap<String, Node>();
		
		for(int i = 0; i < urls.size(); i++) {
			String url = (String) urls.get(i);
			Node node = new Node((String) url);
			nodes.add(node);
			urlMap.put(url, node);
		}
		
		for(Object edge : edges) {
			int parentIndex = (Integer) ((Tuple) edge).get(0);
			int childIndex = (Integer) ((Tuple) edge).get(1);
			Node parentNode = nodes.get(parentIndex);
			Node childNode = nodes.get(childIndex);
			parentNode.getChildren().add(childNode);
		}
		return nodes.get(0);
	}
	
	private class Node {
		private Set<Node> children;
		String url;
		
		public Node(String url) {
			this.url = url;
			children = new HashSet<Node>();
		}
		
		public Set<Node> getChildren() {
			return children;
		}
		
		public String getUrl() {
			return url;
		}
		
		public String toString() {
			return url;
		}
	}
}
