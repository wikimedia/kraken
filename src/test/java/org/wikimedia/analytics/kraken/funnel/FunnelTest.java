package org.wikimedia.analytics.kraken.funnel;

import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;
import org.wikimedia.analytics.kraken.funnel.DemoFunnel;
import org.wikimedia.analytics.kraken.funnel.Funnel;

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
	public final String funnelDefinition = "A,B\n" + "B, C\n" + "D, B\n"
			+ "B, E\n";
	public final String nodeDefinition = "page:";
	private Funnel funnel;

	/**
	 * The first non-trivial funnel is: 
	 * A -> 	->C 
	 * 		B 
	 * D ->		-> E 
	 * where {A,B,C,D,E} are abstract names for the steps in the funnel. 
	 * There are two unique paths in this funnel: {A,B,C} and {D,B,E}
	 */
	public FunnelTest() throws MalformedFunnelException {
		this.funnel = new Funnel(nodeDefinition, funnelDefinition);
	}

	/**
	 * Test get destination vertices.
	 * @throws MalformedFunnelException 
	 */
	@Test
	public final void testGetDestinationVertices() throws MalformedFunnelException {
		List<Node> endVertices = new ArrayList<Node>();
		funnel.getDestinationVertices();
		System.out.println("Number of vertices: " + funnel.endVertices.size());

		endVertices.add(new UserActionNode("C"));
		endVertices.add(new UserActionNode("E"));
		assertTrue(funnel.endVertices.containsAll(endVertices));
	}

	@Test
	public final void testGetStartingVertices() throws MalformedFunnelException {
		List<Node> startVertices = new ArrayList<Node>();
		funnel.getStartingVertices();
		System.out
				.println("Number of vertices: " + funnel.startVertices.size());
		startVertices.add(new UserActionNode("A"));
		startVertices.add(new UserActionNode("D"));
		// for (URL startVertex : funnel.startVertices) {
		// System.out.println(startVertex.toString());
		// }
		// assert funnel.startVertices.equals()
		System.out.println("Found starting Vertices: "
				+ Arrays.toString(funnel.startVertices.toArray()));
		assertTrue(funnel.startVertices.indexOf("A") > -1);
		assertTrue(funnel.startVertices.indexOf("D") > -1);
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
	public final void testFallOutAnalysis() throws MalformedFunnelException {
		ArrayList<Node> path0 = new ArrayList<Node>();
		path0.add(new UserActionNode("A"));
		path0.add(new UserActionNode("B"));
		path0.add(new UserActionNode("C"));

		ArrayList<Node> path1 = new ArrayList<Node>();
		path1.add(new UserActionNode("D"));
		path1.add(new UserActionNode("B"));
		path1.add(new UserActionNode("E"));

		funnel.paths.add(0, path0);
		funnel.paths.add(1, path1);

		HashMap<Integer, Boolean> results = funnel.fallOutAnalysis(funnel.graph);
		assertTrue(results.size() == 2);
		assertTrue(!results.values().contains(false));
	}

	@Test
	public final void testAnalysis() throws MalformedFunnelException {
		// Unfinished test, add code to compare that the results of the two runs
		// are identical.
		DirectedGraph<Node, DefaultEdge> history = DemoFunnel.createFakeUserHistory(100, 250);
		funnel.analysis("fake_user_token", history);
		funnel.analysis("fake_user_token", history);
	}
}
