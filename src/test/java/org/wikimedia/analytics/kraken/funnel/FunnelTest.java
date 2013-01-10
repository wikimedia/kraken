package org.wikimedia.analytics.kraken.funnel;

import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;
import org.wikimedia.analytics.kraken.funnel.DemoFunnel;
import org.wikimedia.analytics.kraken.funnel.Funnel;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for Funnel.
 */
public class FunnelTest {

    /** Create the test case. */
	public final String funnelDefinition = "http://en.wikipedia.org/wiki/A,http://en.wikipedia.org/wiki/B\n" +
			"http://en.wikipedia.org/wiki/B, http://en.wikipedia.org/wiki/C\n" +
			"http://en.wikipedia.org/wiki/D, http://en.wikipedia.org/wiki/B\n" +
			"http://en.wikipedia.org/wiki/B, http://en.wikipedia.org/wiki/E\n";
	public final String nodeDefinition = "page:";
	private Funnel funnel;

	/**
	 * The first non-trivial funnel is:
	 * A ->      C
	 *      B ->
	 * D ->      E
	 * where {A,B,C,D,E} are abstract names for the steps in the funnel.
	 * There are two unique paths in this funnel: {A,B,C} and {D,B,E}
	 */
	public FunnelTest() {
		try {
			this.funnel = new Funnel(nodeDefinition, funnelDefinition);
		} catch (MalformedFunnelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Test get destination vertices.
	 */
	@Test
	public final void testGetDestinationVertices() {
		List<URL> endVertices = new ArrayList<URL>();
		funnel.getDestinationVertices();
		System.out.println("Number of vertices: " + funnel.endVertices.size());
		
		try {
			endVertices.add(new URL("http://en.wikipedia.org/wiki/C"));
			endVertices.add(new URL("http://en.wikipedia.org/wiki/E"));
		} catch (MalformedURLException e) {
			e.printStackTrace(); 
		}
		assertTrue(funnel.endVertices.containsAll(endVertices));
	}

	@Test
	public final void testGetStartingVertices() throws MalformedURLException, MalformedFunnelException {
		List<URL> startVertices = new ArrayList<URL>();
		funnel.getStartingVertices();
		System.out.println("Number of vertices: " + funnel.startVertices.size());
		try {
			startVertices.add(new URL("http://en.wikipedia.org/wiki/A"));
			startVertices.add(new URL("http://en.wikipedia.org/wiki/D"));
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
//		for (URL startVertex : funnel.startVertices) {
//			System.out.println(startVertex.toString());	
//		}
		//assert funnel.startVertices.equals()
		System.out.println("Found starting Vertices: " + Arrays.toString(funnel.startVertices.toArray()));
		assertTrue(funnel.startVertices.indexOf(new URL("http://en.wikipedia.org/wiki/A")) > -1);
		assertTrue(funnel.startVertices.indexOf(new URL("http://en.wikipedia.org/wiki/D")) > -1);
	}
	
	@Test
	public final void testIsDag() {
		assertTrue(funnel.isDag());
	}

	@Test
	public final void testDetermineUniquePaths() {
		funnel.getStartingVertices();
		funnel.getDestinationVertices();
		funnel.determineUniquePaths();
		System.out.println("Unique paths: " + funnel.paths.size());
		assertTrue(funnel.paths.size() == 2);
	}
	@Test
	public final void testFallOutAnalysis() throws MalformedURLException {
		ArrayList<URL> path0 = new ArrayList<URL>();
		path0.add(new URL("http://en.wikipedia.org/wiki/A"));
		path0.add(new URL("http://en.wikipedia.org/wiki/B"));
		path0.add(new URL("http://en.wikipedia.org/wiki/C"));
		
		ArrayList<URL> path1 = new ArrayList<URL>();
		path1.add(new URL("http://en.wikipedia.org/wiki/D"));
		path1.add(new URL("http://en.wikipedia.org/wiki/B"));
		path1.add(new URL("http://en.wikipedia.org/wiki/E"));
		
		funnel.paths.add(0, path0);
		funnel.paths.add(1, path1);
		
		HashMap<Integer, Boolean> results = funnel.fallOutAnalysis(funnel.graph);
		assertTrue(results.size() == 2);
		assertTrue(!results.values().contains(false));
	}
	
	@Test
	public final void testAnalysis() {
		//Unfinished test, add code to compare that the results of the two runs
		//are identical.
		DirectedGraph<URL, DefaultEdge> history = DemoFunnel.createFakeUserHistory(100, 250);
		funnel.analysis(history);
		funnel.analysis(history);
	}
}
