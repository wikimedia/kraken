package org.wikimedia.analytics.kraken.jgraph;

import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for Funnel.
 */
public class FunnelTest {

    /** Create the test case. */
	private Funnel funnel;
	private DirectedGraph<URL, DefaultEdge> history;


	/**
	 * The first non-trivial funnel is:
	 * A ->      C
	 *      B ->
	 * D ->      E
	 * where {A,B,C,D,E} are abstract names for the steps in the funnel.
	 * There are two unique paths in this funnel: {A,B,C} and {D,B,E}
	 */
	public FunnelTest() {
		String funnelDefinition = "http://en.wikipedia.org/A,http://en.wikipedia.org/B," +
				"http://en.wikipedia.org/C;http://en.wikipedia.org/D," +
				"http://en.wikipedia.org/E";
		try {
			this.funnel = new Funnel(funnelDefinition);
		} catch (MalformedFunnelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		this.history = funnel.createFakeUserHistory(100, 250);
	}

	/**
	 * Test get destination vertices.
	 */
	@Test
	public final void testGetDestinationVertices() {
		funnel.getDestinationVertices();
		List<URL> endVertices = new ArrayList<URL>();
		try {
			endVertices.add(new URL("http://en.wikipedia.org/C"));
			endVertices.add(new URL("http://en.wikipedia.org/E"));
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		assert funnel.endVertices.containsAll(endVertices);
	}

	@Test
	public final void testGetStartingVertices() {
		funnel.getStartingVertices();
		List<URL> startVertices = new ArrayList<URL>();
		try {
			startVertices.add(new URL("http://en.wikipedia.org/A"));
			startVertices.add(new URL("http://en.wikipedia.org/D"));
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		assert funnel.startVertices.containsAll(startVertices);
	}
	
	@Test
	public final void testIsDag() {
		assert funnel.isDag();
	}

	@Test
	public final void testDetermineUniquePaths() {
		funnel.determineUniquePaths();
		assert (funnel.paths.size() == 2);
	}
	@Test
	public final void testFallOutAnalysis() {
		HashMap<Integer, Boolean> results = funnel.fallOutAnalysis(funnel.graph);
		assert (results.size() == 2);
		Collection<Boolean> obsValues = results.values();
		Collection<Boolean> testValues = new ArrayList<Boolean>();
		testValues.add(true);
		testValues.add(true);
		assert (obsValues.containsAll(testValues));
	}
}
