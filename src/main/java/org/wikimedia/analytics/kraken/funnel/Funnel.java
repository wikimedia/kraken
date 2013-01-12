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

/*
 * http://meta.wikimedia.org/wiki/Research:Metrics#Funnel_metrics
 */
package org.wikimedia.analytics.kraken.funnel;

import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;
import org.wikimedia.analytics.kraken.utils.SortUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

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
 * are valid Node's.
 *
 * Browsing History
 * The browsing history should be a time sorted list of Node's visited by a
 * single visitor, ideally during one session.
 */
public class Funnel {

	/** The funnel. */
	public Funnel funnel;
	/** The graph. */
	public DirectedGraph<Node, DefaultEdge> graph;

	/** The start vertices. */
	public List<Node> startVertices;

	/** The end vertices. */
	public List<Node> endVertices;

	/** The paths. */
	public List<ArrayList<Node>> paths;

	/** Edge definition */
	public List<String> nodeDefinition = new ArrayList<String>();

	/**
	 * Constructor for the funnel.
	 *
	 * @throws MalformedFunnelException the malformed funnel exception
	 * @throws MalformedNodeException the malformed url exception
	 */
	public Funnel() throws MalformedFunnelException {
		this("page","A,B\n" +
				"B, C\n" +
				"D, B\n" +
				"B, E\n");
	}

	/**
	 * Constructor for the funnel.
	 *
	 * @param funnelDefinition the funnel definition
	 * @param nodeDefinition a String in the format <key1>:<key2>:<keyn>, these 
	 * keys *should* map to the keys as defined in the EventLogging schema that 
	 * is used for this particular funnel and should be present in the Node query
	 * string.
	 * @throws MalformedFunnelException the malformed funnel exception
	 * @throws MalformedNodeException the malformed url exception
	 */
	public Funnel(String nodeDefinition, String funnelDefinition) throws MalformedFunnelException {
		this.paths = new ArrayList<ArrayList<Node>>();
		//this.graph = new DefaultDirectedGraph<Node, DefaultEdge>(DefaultEdge.class);
		this.setNodeDefinition(nodeDefinition);
		this.graph = this.constructFunnelGraph(funnelDefinition);
		if (this.graph.edgeSet().size() == 0 || this.graph.vertexSet().size() < 2) {
			System.out.println(this.graph.toString());
			throw new MalformedFunnelException("A funnel needs to have two connected nodes at the very minimum.");
		}
		if (!isDag()) {
			throw new MalformedFunnelException("A funnel needs to have two connected nodes at the very minimum.");
		}
	}

	public void setNodeDefinition(String nodeDefinition) throws MalformedFunnelException {
		for (String component : nodeDefinition.split(":")) {
			this.nodeDefinition.add(component);
		}
		if (this.nodeDefinition.size() == 0) {
			throw new MalformedFunnelException("Your node definition does not use use the colon as a separator.");
		}
	}

	/**
	 * Construct graph.
	 *
	 * @param funnelDefinition the edges
	 * @return the directed graph
	 * @throws MalformedFunnelException 
	 * @throws PatternSyntaxException 
	 */
	public final DirectedGraph<Node, DefaultEdge> constructFunnelGraph(String funnelDefinition) throws PatternSyntaxException, MalformedFunnelException {
		String[] edgelist = funnelDefinition.split(";");
		DirectedGraph<Node, DefaultEdge> dg = new DefaultDirectedGraph<Node, DefaultEdge>(DefaultEdge.class);
		Node source = null;
		Node target = null;
		for (String  edgeData : edgelist) {
			String[] edge = edgeData.split(",");
				source = new FunnelNode(edge[0]);
				target = new FunnelNode(edge[1]);
			}
			if (!dg.containsVertex(source)) {
				dg.addVertex(source);
			}
			if (!dg.containsVertex(target)) {
				dg.addVertex(target);
			}
			if (!dg.containsEdge(source, target)) {
				dg.addEdge(source, target);
			}
		return dg;
	}

	/**
	 * Simple wrapper script that conducts all the steps to do a funnel
	 * analysis.
	 */
	public final void analysis(DirectedGraph<Node, DefaultEdge> history) {
		this.getStartingVertices();
		this.getDestinationVertices();
		//System.out.println("Funnel is a DAG (true/false): " + result);
		//System.out.println("Graph summary: " + history.toString());
		this.determineUniquePaths();
		HashMap<Integer, Integer> results = this.fallOutAnalysisDetailed(history);
		this.outputFallOutAnalysisResults(results);
	}

	/**
	 * Determine all the unique paths between all the {@link startVertices}
	 * and {@link endVertices}.
	 */
	public final void determineUniquePaths() {
		for (Node startVertex : startVertices) {
			System.out.println("Start vertex: " + startVertex.toString());
			DepthFirstIterator<Node, DefaultEdge> dfi = new DepthFirstIterator<Node, DefaultEdge>(graph, startVertex);
			ArrayList<Node> path = new ArrayList<Node>();
			while (dfi.hasNext()) {
				Node url = dfi.next();
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
		startVertices = new ArrayList<Node>();
		Set<Node> vertices = graph.vertexSet();
		int indegree;
		for (Node vertex: vertices) {
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
		endVertices = new ArrayList<Node>();
		Set<Node> vertices = graph.vertexSet();
		int outdegree;
		for (Node vertex : vertices) {
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
	public HashMap<Integer, Boolean> fallOutAnalysis(DirectedGraph<Node, DefaultEdge> history) {
		HashMap<Integer, Boolean> results = new HashMap<Integer, Boolean>();
		int i = 0;
		int j = 0;
		int p = 0;
		for (ArrayList<Node> path : paths) {
			for (i = 0; i < path.size() - 1; i++) {
				j = i + 1;
				Node source = path.get(i);
				Node target = path.get(j);
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
	public HashMap<Integer, Integer> fallOutAnalysisDetailed(DirectedGraph<Node, DefaultEdge> history) {
		HashMap<Integer, Integer> results = new HashMap<Integer, Integer>();
		int i = 0;
		int j = 0;
		int p = 0;
		for (ArrayList<Node> path : paths) {
			for (i = 0; i < path.size() - 1; i++) {
				j = i + 1;
				Node source = path.get(i);
				Node target = path.get(j);
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
	 * Checks if funnel is a DAG.
	 *
	 * @return true if the funnel is valid directed acyclical graph (DAG) else
	 * return false.
	 */
	public boolean isDag() {
		CycleDetector<Node, DefaultEdge> cycle = new CycleDetector<Node, DefaultEdge>(this.graph);
		boolean result = cycle.detectCycles();
		if (result) {
			return false; //Graph is *not* acyclical
		} else {
			return true;
		}
	}

	public DirectedGraph<Node, DefaultEdge> constructUserGraph(
			Map<String, HashMap<Date, JsonObject>> jsonData) {
		DirectedGraph<Node, DefaultEdge> dg = new DefaultDirectedGraph<Node, DefaultEdge>(DefaultEdge.class);
		Node source;
		Node target;
		for (Entry<String, HashMap<Date, JsonObject>> kv : jsonData.entrySet()) {

			Set<Date> datesSet = kv.getValue().keySet();
			List<Date> dates = SortUtils.asSortedList(datesSet);
			int i;
			int j;
			for (i=0; i + 1 < dates.size(); i++) {
				j = i + 1;
				JsonElement sourceJson = kv.getValue().get(dates.get(i)).getAsJsonObject().get("action");
				JsonElement targetJson = kv.getValue().get(dates.get(j)).getAsJsonObject().get("action");
				if (!sourceJson.isJsonNull() && !targetJson.isJsonNull()) {
					try {
						source = new UserActionNode(sourceJson.toString());
						target = new UserActionNode(sourceJson.toString());
						if (!dg.containsVertex(source)) {
							dg.addVertex(source);
						}
						if (!dg.containsVertex(target)) {
							dg.addVertex(target);
						}
						if (!dg.containsEdge(source, target)) {
							dg.addEdge(source, target);
						}
					} catch (MalformedFunnelException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		return dg;
	}
}
