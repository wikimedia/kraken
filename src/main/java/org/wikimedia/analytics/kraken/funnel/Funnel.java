/**
 *Copyright (C) 2012  Wikimedia Foundation
 *
 *This program is free software; you can redistribute it and/or
 *modify it under the terms of the GNU General Public License
 *as published by the Free Software Foundation; either version 2
 *of the License, or (at your option) any later version.
 *
 *This program is distributed in the hope that it will be useful,
 *but WITHOUT ANY WARRANTY; without even the implied warranty of
 *MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *GNU General Public License for more details.
 *
 *You should have received a copy of the GNU General Public License
 *along with this program; if not, write to the Free Software
 *Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 *
 * @version $Id: $Id
 */

package org.wikimedia.analytics.kraken.funnel;

import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
/**
 * Funnel fun!.
 *
 * This class provides automated funnel analysis using a social network analysis
 * approach. Two parameters need to be provided:
 * 1) The Funnel definition
 * 2) A browsing history of a single user
 *
 * Funnel Definition
 * A funnel is defined as a Directed Acyclical Graph (DAG), the simplest funnel
 * would look like: A -> B -> C
 * You have to provide the different paths that make up a funnel. The {A,B,C}
 * funnel should be provided as "A,B,C;". Make sure that all steps in the funnel
 * are valid URL's.
 *
 * Browsing History
 * The browsing history should be a time sorted list of URL's visited by a
 * single visitor, ideally during one session.
 */
public class Funnel {

	/** The funnel. */
	Funnel funnel;
	/** The graph. */
	DirectedGraph<URL, DefaultEdge> graph;

	/** The start vertices. */
	List<URL> startVertices;

	/** The end vertices. */
	List<URL> endVertices;

	/** The paths. */
	List<ArrayList<URL>> paths;
	
	/** Edge definition */
	List<String> nodeDefinition = new ArrayList<String>();

	/**
	 * Constructor for the funnel.
	 *
	 * @throws MalformedFunnelException the malformed funnel exception
	 * @throws MalformedURLException the malformed url exception
	 */
	public Funnel() throws MalformedFunnelException, MalformedURLException {
		this("","http://en.wikipedia.org/wiki/A,http://en.wikipedia.org/wiki/B\n" +
				"http://en.wikipedia.org/wiki/B, http://en.wikipedia.org/wiki/C\n" +
				"http://en.wikipedia.org/wiki/D, http://en.wikipedia.org/wiki/B\n" +
				"http://en.wikipedia.org/wiki/B, http://en.wikipedia.org/wiki/E\n"
				);
	}

	/**
	 * Constructor for the funnel.
	 *
	 * @param funnelDefinition the funnel definition
	 * @param nodeDefinition a String in the format <key1>:<key2>:<keyn>, these 
	 * keys *should* map to the keys as defined in the EventLogging schema that 
	 * is used for this particular funnel and should be present in the URL query
	 * string.
	 * @throws MalformedFunnelException the malformed funnel exception
	 * @throws MalformedURLException the malformed url exception
	 */
	public Funnel(String nodeDefinition, String funnelDefinition) throws MalformedFunnelException, MalformedURLException{
		paths = new ArrayList<ArrayList<URL>>();
		graph = new DefaultDirectedGraph<URL, DefaultEdge>(DefaultEdge.class);
		this.setnodeDefinition(nodeDefinition);
		this.createFunnel(funnelDefinition);
	}

	public void setnodeDefinition(String nodeDefinition) throws MalformedFunnelException {
		for (String component : nodeDefinition.split(":")) {
			this.nodeDefinition.add(component);
		}
		if (this.nodeDefinition.size() == 0) {
			throw new MalformedFunnelException("Your edge definition does not use use the colon as a separator.");
		}
	}
	
	/**
	 * Construct graph.
	 *
	 * @param edges the edges
	 * @return the directed graph
	 * @throws MalformedURLException the malformed url exception
	 */
	public final DirectedGraph<URL, DefaultEdge> constructGraph(String edges) throws MalformedURLException{
		String[] edgelist = edges.split("\n");
		DirectedGraph<URL, DefaultEdge> dg = new DefaultDirectedGraph<URL, DefaultEdge>(DefaultEdge.class);
		for (String  edgeData : edgelist) {
			String[] edge = edgeData.split(",");
			URL source = new URL(edge[0]);
			URL target = new URL(edge[1]);
			if (!dg.containsVertex(source)) {
				dg.addVertex(source);
			}
			if (!dg.containsVertex(target)) {
				dg.addVertex(target);
			}
			if (!dg.containsEdge(source,target)) {
				dg.addEdge(source, target);
			}
		}
		return dg;
	}

	/**
	 * Simple wrapper script that conducts all the steps to do a funnel
	 * analysis.
	 */
	public final void analysis(DirectedGraph<URL, DefaultEdge> history) {
		this.getStartingVertices();
		this.getDestinationVertices();
		//System.out.println("Funnel is a DAG (true/false): " + result);
		//System.out.println("Graph summary: " + history.toString());
		this.determineUniquePaths();
		HashMap<Integer, Integer> results = this.fallOutAnalysisDetailed(history);
		this.outputFallOutAnalysisResults(results);
	}

	/**
	 * This function takes as input different fields from the EventLogging 
	 * extension  and turns it into a single string that is in the form:
	 * client:languagecode:project:namespace:page:event {@link Node}
	 *
	 * @param fproject the project variable as generated by the EventLogging extension.
	 * @param fnamespace the namespace variable as generated by the EventLogging extension.
	 * @param fpage the page variable as generated by the EventLogging extension.
	 * @param fevent the event variable as generated by the EventLogging extension.
	 */	
	public String createEdge(String queryString) {
		URI url;
		List<NameValuePair> params = null;
		try {
			url = new URI("http://www.wikipedia.org/" + queryString);
			params = URLEncodedUtils.parse(url, "UTF-8");
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		StringBuilder sb = new StringBuilder();
		Integer i = 1;
		for (String key: this.nodeDefinition) {
			for (NameValuePair param : params) {
				if (key.equals(param.getName())) {
					sb.append(param.getValue());
					break;
				}
			if (i < this.nodeDefinition.size()) {
				sb.append(":");
			}
			i++;
			}
		}
		return sb.toString();
	}


	/**
	 * Determine all the unique paths between all the {@link startVertices}
	 * and {@link endVertices}.
	 */
	public final void determineUniquePaths() {
		for (URL startVertex : startVertices) {
			System.out.println("Start vertex: " + startVertex.toString());
			DepthFirstIterator<URL, DefaultEdge> dfi = new DepthFirstIterator<URL, DefaultEdge>(graph, startVertex);
			ArrayList<URL> path = new ArrayList<URL>();
			while (dfi.hasNext()) {
				URL url = dfi.next();
				path.add(url);
				System.out.println("--> " + url.toString());
				if (endVertices.contains(url)) {
					break;
				}

			}
			System.out.println(path.toString());
			paths.add(path);
		}
	}

	/**
	 * Retrieve all the possible start vertices from the funnel.
	 *
	 * @return the starting vertices
	 */
	public final void getStartingVertices() {
		startVertices = new ArrayList<URL>();
		Set<URL> vertices = graph.vertexSet();
		int indegree;
		for (URL vertex: vertices) {
			indegree = graph.inDegreeOf(vertex);
			if (indegree == 0) {
				startVertices.add(vertex);
			}
		}
	}

	/**
	 * Retrieve all the possible destination vertices from the funnel.
	 *
	 * @return the destination vertices
	 */
	public final void getDestinationVertices() {
		endVertices = new ArrayList<URL>();
		Set<URL> vertices = graph.vertexSet();
		int outdegree;
		for (URL vertex : vertices) {
			outdegree = graph.outDegreeOf(vertex);
			if (outdegree == 0) {
				endVertices.add(vertex);
			}
		}
	}

	/**
	 * Fall out analysis.
	 *
	 * @param history the history
	 * @return the hash map
	 */
	public HashMap<Integer, Boolean> fallOutAnalysis(DirectedGraph<URL, DefaultEdge> history) {
		HashMap<Integer, Boolean> results = new HashMap<Integer, Boolean>();
		int i = 0;
		int j = 0;
		int p = 0;
		for (ArrayList<URL> path : paths) {
			for (i = 0; i < path.size() - 1; i++) {
				j = i + 1;
				URL source = path.get(i);
				URL target = path.get(j);
				DefaultEdge edge = history.getEdge(source, target);
				if (edge == null) {
					break;
				}
			}
			if (j == path.size()-1) {
				results.put(p, true);
			} else {
				results.put(p, false);
			}
			j = 0;
			p++;
		}
		return results;
	}

	/**
	 * Detailed fallout analysis.
	 *
	 * @param history the browser history of a single user
	 * @return the hashmap< integer, integer>. The key is the path id and the
	 * value is the last reached step within a path of the funnel.
	 */
	public HashMap<Integer, Integer> fallOutAnalysisDetailed(DirectedGraph<URL, DefaultEdge> history) {
		HashMap<Integer, Integer> results = new HashMap<Integer, Integer>();
		int i = 0;
		int j = 0;
		int p = 0;
		for (ArrayList<URL> path : paths) {
			for (i = 0; i < path.size() - 1; i++) {
				j = i + 1;
				URL source = path.get(i);
				URL target = path.get(j);
				DefaultEdge edge = history.getEdge(source, target);
				if (edge == null) {
					break;
				}
			}
			if (j == path.size()) {
				i = j;
			}
			results.put(p, i);
			j = 0;
			p++;
		}
		return results;
	}

	/**
	 * Output the results of the fallout analysis.
	 *
	 * @param results the results
	 */
	public void outputFallOutAnalysisResults(HashMap<Integer, Integer> results) {
		for (Entry<Integer, Integer> result : results.entrySet()) {
			Integer key = result.getKey();
			Integer value = result.getValue();
			System.out.println("Path id: " + key.toString() + "; Destination: " + value.toString());
		}
	}

	/**
	 * Creates the funnel.
	 *
	 * @param funnelDefinition the funnel definition
	 * @throws MalformedFunnelException the malformed funnel exception
	 * @throws MalformedURLException the malformed url exception
	 */
	public void createFunnel(String funnelDefinition) throws MalformedFunnelException, MalformedURLException{
		graph = constructGraph(funnelDefinition);
		if (graph.edgeSet().size() == 0 || graph.vertexSet().size() < 2) {
			throw new MalformedFunnelException("A funnel needs to have two connected nodes at the very minimum.");
		}
		if (!isDag()) {
			throw new MalformedFunnelException("A funnel needs to have two connected nodes at the very minimum.");
		}
	}

	/**
	 * Checks if funnel is a DAG.
	 *
	 * @return true if the funnel is valid directed acyclical graph (DAG) else
	 * return false.
	 */
	boolean isDag() {
		CycleDetector<URL, DefaultEdge> cycle = new CycleDetector<URL, DefaultEdge>(this.graph);
		boolean result = cycle.detectCycles();
		if (result) {
			return false; //Graph is *not* acyclical
		} else {
			return true;
		}
	}
}
