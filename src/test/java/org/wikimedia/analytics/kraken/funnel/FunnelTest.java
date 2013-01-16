package org.wikimedia.analytics.kraken.funnel;

import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;
import org.wikimedia.analytics.kraken.funnel.DemoFunnel;
import org.wikimedia.analytics.kraken.funnel.Funnel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
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
		FunnelPath path0 = new FunnelPath(0);
        FunnelNode A = new FunnelNode("A");
        FunnelNode B = new FunnelNode("B");
        FunnelNode C = new FunnelNode("C");
        FunnelNode D = new FunnelNode("D");
        FunnelNode E = new FunnelNode("E");

		path0.nodes.add(A);
		path0.nodes.add(B);
		path0.nodes.add(C);

		FunnelPath path1 = new FunnelPath(1);
		path1.nodes.add(D);
		path1.nodes.add(B);
		path1.nodes.add(E);

		funnel.paths.add(0, path0);
		funnel.paths.add(1, path1);

		Analysis analysis = new Analysis();
		DirectedGraph<Node, DefaultEdge> history = null;
		Result result = analysis.run("fake_user_token", history, funnel);
		assertTrue(result.getHasFinishedFunnel());
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
