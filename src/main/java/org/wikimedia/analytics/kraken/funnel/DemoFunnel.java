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
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;

public class DemoFunnel {

	public DemoFunnel() {
	}

	/**
	 * The main method.
	 *
	 * @param args the args, right now we only handle {@link funnelDefinition}
	 * @throws MalformedFunnelException the malformed funnel exception
	 * @throws MalformedURLException the malformed url exception
	 */
	public static void main(String[] args) throws MalformedFunnelException, MalformedURLException {
		DirectedGraph<URL, DefaultEdge> history = createFakeUserHistory(100, 250);
		Funnel funnel;
		if (args.length > 1) {
			funnel = new Funnel(args[0], args[1]);
		} else {
			System.out.println("No funnel supplied, I will use the example funnel.");
			funnel = new Funnel();
		}
		funnel.analysis(history);
	}
	/**
	 * Creates a fake browse history for a fake person.
	 *
	 * @param numberNodes the number nodes
	 * @param numberEdges the number edges
	 * @return the directed graph< url, default edge>
	 */
	public static DirectedGraph<URL, DefaultEdge> createFakeUserHistory(Integer numberNodes, Integer numberEdges){
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
}
