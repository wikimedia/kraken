package org.wikimedia.analytics.kraken.funnel;

import com.google.gson.JsonObject;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.junit.Test;
import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for Funnel.
 */
public class FunnelTest {

	/** Create the test case. */
	public final String funnelDefinition = "event=A,event=B;" +
                                           "event=B,event=C;" +
                                           "event=D,event=B;" +
			                               "event=B,event=E;";
	public final String nodeDefinition = "event=page";
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
        Map<String, String> nodeDefinition = new HashMap<String, String>();
        List<Node> endVertices = new ArrayList<Node>();

		endVertices.add(new FunnelNode("event=C"));
		endVertices.add(new FunnelNode("event=E"));

		assertTrue(funnel.endVertices.containsAll(endVertices));
	}

    @Test
    public final void testFunnelNodeEqualsUserActionNode() throws MalformedFunnelException {
        FunnelNode f = new FunnelNode("LANGUAGE=en");
        JsonObject json = new JsonObject();
        json.addProperty("language", "en");
        UserActionNode u = new UserActionNode(json);

        assertTrue(f.equals(u));
    }

    @Test
    public final void testFunnelNodeDoesNotEqualUserActionNode() throws MalformedFunnelException {
        FunnelNode f = new FunnelNode("LANGUAGE=en");
        JsonObject json = new JsonObject();
        json.addProperty("language", "de");
        UserActionNode u = new UserActionNode(json);

        assertFalse(f.equals(u));
    }

	@Test
	public final void testGetStartingVertices() throws MalformedFunnelException {
		assertTrue(funnel.startVertices.contains(new FunnelNode("event=A")));
        assertTrue(funnel.startVertices.contains(new FunnelNode("event=D")));
	}

	@Test
	public final void testIsDag() {
		assertTrue(funnel.isDag());
	}

	@Test
	public final void testDetermineUniquePaths() {
		assertTrue(funnel.paths.size() == 4);
	}

	@Test
	public final void testFallOutAnalysis() throws MalformedFunnelException {
		Analysis analysis = new Analysis();
		DirectedGraph<Node, DefaultEdge> history = new DefaultDirectedGraph<Node, DefaultEdge>(DefaultEdge.class);

        JsonObject json = new JsonObject();
        json.addProperty("event", "A");
        UserActionNode userGoesToA = new UserActionNode(json);

        json = new JsonObject();
        json.addProperty("event", "B");
        UserActionNode userGoesToB = new UserActionNode(json);

        json = new JsonObject();
        json.addProperty("event", "C");
        UserActionNode userGoesToC = new UserActionNode(json);

        history.addVertex(userGoesToA);
        history.addVertex(userGoesToB);
        history.addVertex(userGoesToC);
        history.addEdge(userGoesToA, userGoesToB);
        history.addEdge(userGoesToB, userGoesToC);

		Result result = analysis.run("fake_user_token", history, funnel);
		//assertTrue(result.getHasFinishedFunnel());
	}

	@Test
	public final void testAnalysis() throws MalformedFunnelException {
		// Unfinished test, add code to compare that the results of the two runs
		// are identical.
        DemoFunnel demoFunnel = new DemoFunnel();
		DirectedGraph<Node, DefaultEdge> history = demoFunnel.createFakeUserHistory(100, 250);
		funnel.analysis("fake_user_token", history);
		funnel.analysis("fake_user_token", history);
	}
}
