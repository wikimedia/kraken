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

import org.apache.commons.lang3.tuple.Pair;
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
	public List<FunnelPath> paths = new ArrayList<FunnelPath>();

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
		//this.graph = new DefaultDirectedGraph<Node, DefaultEdge>(DefaultEdge.class);
		this.setNodeDefinition(nodeDefinition);
		System.out.println("Node definition: " + nodeDefinition);
		System.out.println("Funnel definition: " + funnelDefinition);
		this.graph = this.constructFunnelGraph(funnelDefinition);
		if (this.graph.edgeSet().size() == 0 || this.graph.vertexSet().size() < 2) {
			System.out.println(this.graph.toString());
			throw new MalformedFunnelException("A funnel needs to have two connected nodes at the very minimum.");
		}
		if (!isDag()) {
			throw new MalformedFunnelException("A funnel needs to have two connected nodes at the very minimum.");
		}
		this.getStartingVertices();
		this.getDestinationVertices();
		this.determineUniquePaths();
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
		//TODO: probably faster to replace ArrayList with HashMap<String, Node>
		ArrayList<Node> nodes = new ArrayList<Node>();
		Node source = null;
		Node target = null;
		for (String  edgeData : edgelist) {
			String[] edge = edgeData.split(",");
			source = new FunnelNode(edge[0]);
			target = new FunnelNode(edge[1]);

			//			System.out.println("Constructing edge: " + edgeData.toString());
			//			System.out.println("Graph contains: " + source + " :" + dg.containsVertex(source));
			//			System.out.println("Graph contains: " + target + " :" + dg.containsVertex(target));
			//			System.out.println("SOURCE: " + source.toString());
			//			System.out.println("TARGET: " + target.toString());

			//Curiously enough, there don't seem to be a method in jGraphT to get
			//a vertex from a graph, hence I keep an ArrayList of nodes that 
			//already exist. 
			if (!nodes.contains(source)) {
				dg.addVertex(source);
				nodes.add(source);
			} else {
				source = nodes.get(nodes.indexOf(source));
			}

			if (!nodes.contains(target)) {
				dg.addVertex(target);
				nodes.add(target);
			} else {
				target = nodes.get(nodes.indexOf(target));
			}
			//			System.out.println("SOURCE: " + source.toString());
			//			System.out.println("TARGET: " + target.toString());

			if (!dg.containsEdge(source, target)) {
				dg.addEdge(source, target);
			}
			System.out.println("Graph Summary: " + dg.toString());

		}
		return dg;
	}

	/**
	 * Simple wrapper script that conducts all the steps to do a funnel
	 * analysis.
	 * @param usertoken 
	 */
	public final void analysis(String userToken, DirectedGraph<Node, DefaultEdge> history) {
		Analysis analysis = new Analysis();
		Result result = analysis.run(userToken,history, funnel);
		analysis.printResults(userToken, result);
	}

	/**
	 * Determine all the unique paths between all the {@link startVertices}
	 * and {@link endVertices}.
	 */
	public final void determineUniquePaths() {
		int i = 0;
		for (Node startVertex : startVertices) {
			System.out.println("Start vertex: " + startVertex.toString());
			DepthFirstIterator<Node, DefaultEdge> dfi = new DepthFirstIterator<Node, DefaultEdge>(graph, startVertex);
			FunnelPath path = new FunnelPath(i);
			while (dfi.hasNext()) {
				Node node = dfi.next();
				path.nodes.add(node);
//				System.out.println("--> " + node.toString());
				if (endVertices.contains(node)) {
					break;
				}
			}
			System.out.println(path.toString());
			i++;
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
	 * Checks if funnel is a DAG.
	 *
	 * @return true if the funnel is valid directed acyclic graph (DAG) else
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

	/*
	 * This function has input a map with as key the usertoken string and as 
	 * value another map. In this second map, the key is a datetime and the 
	 * value is the @{link jsonObject} that contains the actual eventLogging 
	 * data for that token/timestamp combination.
	 * 
	 * This function iterates over all usertokens, over all dates and constructs
	 * for each usertoken a new graph representing the actions of that
	 * user token.
	 * 
	 * @returns a map with as key the usertoken and as value the graph 
	 * representing their actions.
	 */
	public Map<String, DirectedGraph<Node, DefaultEdge>> constructUserGraph(
			Map<String, HashMap<Date, JsonObject>> jsonData) {
		Map<String, DirectedGraph<Node, DefaultEdge>> graphs = new HashMap<String, DirectedGraph<Node, DefaultEdge>>();
		Node source;
		Node target;
		for (Entry<String, HashMap<Date, JsonObject>> kv : jsonData.entrySet()) {
			DirectedGraph<Node, DefaultEdge> dg = new DefaultDirectedGraph<Node, DefaultEdge>(DefaultEdge.class);
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
						e.printStackTrace();
					}
				}
			}
			graphs.put(kv.getKey(), dg);
		}
		return graphs;
	}
}
