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

package org.wikimedia.analytics.kraken.jgraph;

import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;

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
	Funnel funnel;
	/** The graph. */
	DirectedGraph<URL, DefaultEdge> graph;

	/** The start vertices. */
	List<URL> startVertices;

	/** The end vertices. */
	List<URL> endVertices;

	/** The paths. */
	List<ArrayList<URL>> paths;

	/**
	 * Constructor for the funnel.
	 * @throws MalformedFunnelException
	 * @throws MalformedURLException 
	 */
	public Funnel() throws MalformedFunnelException, MalformedURLException {
		String exampleFunnelDefinition = "http://en.wikipedia.org/wiki/A,http://en.wikipedia.org/wiki/B\n" +
			"http://en.wikipedia.org/wiki/B, http://en.wikipedia.org/wiki/C\n" +
			"http://en.wikipedia.org/wiki/C, http://en.wikipedia.org/wiki/D\n";
		String funnelDefinition = "http://en.wikipedia.org/wiki/A,http://en.wikipedia.org/wiki/B\n" +
			"http://en.wikipedia.org/wiki/B, http://en.wikipedia.org/wiki/C\n" +
			"http://en.wikipedia.org/wiki/D, http://en.wikipedia.org/wiki/B\n" +
			"http://en.wikipedia.org/wiki/B, http://en.wikipedia.org/wiki/E\n";
		this.createFunnel(funnelDefinition);
	}

	/**
	 * Constructor for the funnel.
	 * @param funnelDefinition
	 * @throws MalformedFunnelException
	 * @throws MalformedURLException 
	 */
	public Funnel(String funnelDefinition) throws MalformedFunnelException, MalformedURLException{
		graph = new DefaultDirectedGraph<URL, DefaultEdge>(DefaultEdge.class);
		this.createFunnel(funnelDefinition);
	}

	/**
	 * The main method.
	 *
	 * @param args the args, right now we only handle {@link funnelDefinition}
	 * @throws MalformedFunnelException the malformed funnel exception
	 * @throws MalformedURLException the malformed url exception
	 */
	public static void main(String[] args) throws MalformedFunnelException, MalformedURLException {
		Funnel funnel;
		if (args.length > 0) {
			funnel = new Funnel(args[0]);
		} else {
			System.out.println("No funnel supplied, I will use the example funnel.");
			funnel = new Funnel();
		}
		funnel.analysis();
	}

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
	public final void analysis() {
		this.getStartingVertices();
		this.getDestinationVertices();
		//System.out.println("Funnel is a DAG (true/false): " + result);
		DirectedGraph<URL, DefaultEdge> history = this.createFakeUserHistory(100, 250);
		System.out.println("Graph summary: " + history.toString());
		this.determineUniquePaths();
		HashMap<Integer, Integer> results = this.fallOutAnalysisDetailed(history);
		this.outputFallOutAnalysisResults(results);
	}

	/**
	 * Creates a fake browse history for a fake person.
	 *
	 * @return the directed graph< url, default edge>
	 */
	public DirectedGraph<URL, DefaultEdge> createFakeUserHistory(Integer numberNodes, Integer numberEdges){
		String baseUrl = "http://en.wikipedia.org/wiki/";
		String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		char path;
		Random rnd = new Random();
		DirectedGraph<URL, DefaultEdge> dg = new DefaultDirectedGraph<URL, DefaultEdge>(DefaultEdge.class);

		// Create fake URL's and use them to seed as the nodes in a graph
		for (int i = 0; i < numberNodes; i++) {
			path = alphabet.charAt(rnd.nextInt(alphabet.length()));
			try {
				URL url = new URL(baseUrl + path);
				if (!dg.containsVertex(url)) {
					dg.addVertex(url);
				}
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		}

		// Add random edges between the fake URL's to create a fake browsing history
		List<URL> vertices = new ArrayList<URL>(dg.vertexSet());
		for (int i = 0; i < numberEdges; i++) {
			URL source = vertices.get(rnd.nextInt(vertices.size()));
			URL target = vertices.get(rnd.nextInt(vertices.size()));
			dg.addEdge(source, target);
		}
		return dg;
	}

	/**
	 * Determine all the unique paths between all the {@link startVertices}
	 * and {@link endVertices}.
	 */
	public final void determineUniquePaths() {
		paths = new ArrayList<ArrayList<URL>>();
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
			if (j == path.size()) {
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
	 * @param funnelDefinition, a string where the different steps are separated
	 * by a comma and the end of a path is indicated using a semi-colon. 
	 * @throws MalformedFunnelException the malformed funnel exception
	 * @throws MalformedURLException 
	 */
	public void createFunnel(String funnelDefinition) throws MalformedFunnelException, MalformedURLException{
		graph = constructGraph(funnelDefinition);
		if (graph.edgeSet().size() == 0 || graph.vertexSet().size() < 2) {
			throw new MalformedFunnelException("A funnel needs to have two connected nodes at the very minimum.");
		}
		if (!isDag()) {
			throw new MalformedFunnelException("A funnel needs to have two connected nodes at the very minimum.");
		}
		
//		String[] paths = funnelDefinition.split(";");
//		HashMap<String, URL> map = new HashMap<String, URL>();
//
//		for (String path : paths) {
//			String[] vertices = path.split(",");
//			System.out.println(Arrays.toString(vertices));
//			if (vertices.length == 1) {
//				throw new MalformedFunnelException("A funnel needs to have two nodes at the very minimum.");
//			}
//
//			// Add vertices to the funnel
//			for (String vertex : vertices) {
//				try {
//					URL url = new URL(vertex);
//					graph.addVertex(url);
//					map.put(vertex, url);
//				} catch (MalformedURLException e) {
//					throw new MalformedFunnelException(e);
//				}
//			}
//
//			// Add edges between the vertices in the funnel
//			int j;
//			for (int i = 0; i < vertices.length - 1; i++) {
//				j = i + 1;
//				URL source = map.get(vertices[i]);
//				URL target = map.get(vertices[j]);
//				graph.addEdge(source, target);
//			}
//		}
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
