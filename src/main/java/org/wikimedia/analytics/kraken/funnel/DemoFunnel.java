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

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;

public class DemoFunnel {
    Map<String, String> nodeDefinition = new HashMap<String, String>();

	/**
	 * The main method.
	 *
	 * @param args the args, right now we only handle {@link Funnel}
	 * @throws MalformedFunnelException the malformed funnel exception
	 * @throws MalformedURLException the malformed url exception
	 */
	public static void main(String[] args) throws MalformedFunnelException, MalformedURLException {
		Funnel funnel;
        DemoFunnel demofunnel = new DemoFunnel();
		DirectedGraph<Node, DefaultEdge> history = demofunnel.createFakeUserHistory(100, 250);
		
		if (args.length == 2) {
			funnel = new Funnel(args[0], args[1]);
		} else {
			System.out.println("No funnel supplied, I will use the example funnel.");
			funnel = new Funnel();
		}
		funnel.analysis("fake_user_token", history);
	}
	/**
	 * Creates a fake browse history for a fake person.
	 *
	 * @param numberNodes the number nodes
	 * @param numberEdges the number edges
	 * @return the directed graph< node, default edge>
	 * @throws MalformedFunnelException 
	 */
	public DirectedGraph<Node, DefaultEdge> createFakeUserHistory(int numberNodes, int numberEdges) throws MalformedFunnelException{
		String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		char path;
		Random rnd = new Random();
		DirectedGraph<Node, DefaultEdge> dg = new DefaultDirectedGraph<Node, DefaultEdge>(DefaultEdge.class);

		// Create fake nodes and use them to seed the graph
		for (int i = 0; i < numberNodes; i++) {
			path = alphabet.charAt(rnd.nextInt(alphabet.length()));
//			Node node = new UserActionNode(Character.toString(path));
//			if (!dg.containsVertex(node)) {
//				dg.addVertex(node);
//			}
		}

		// Add random edges between the fake nodes to create a fake browsing history
		List<Node> vertices = new ArrayList<Node>(dg.vertexSet());
		for (int i = 0; i < numberEdges; i++) {
			Node source = vertices.get(rnd.nextInt(vertices.size()));
			Node target = vertices.get(rnd.nextInt(vertices.size()));
			dg.addEdge(source, target);
		}
		return dg;
	}
}
