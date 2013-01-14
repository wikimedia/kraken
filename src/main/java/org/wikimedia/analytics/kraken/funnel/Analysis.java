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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

public class Analysis {

	public boolean hasBouncedFromFunnel(
			DirectedGraph<Node, DefaultEdge> history, Node sourceVertex,
			Node targetVertex) {
		return history.containsEdge(sourceVertex, targetVertex);
	}

	public Pair<FunnelPath, Boolean> hasCompletedFunnel(DirectedGraph<Node, DefaultEdge> history,
			Funnel funnel) {
		FunnelPath path = null;
		Iterator<FunnelPath> fp = funnel.paths.listIterator();
		while (fp.hasNext()) {
			path = fp.next();
			Iterator<Pair<Node, Node>> it = path.iterator();
			Pair<Node, Node> pair;
			List<Boolean> results = new ArrayList<Boolean>();
			while (it.hasNext()) {
				pair = it.next();
				results.add(hasBouncedFromFunnel(history, pair.getLeft(),
						pair.getRight()));
			}
			if (!results.contains(false)) {
				return Pair.of(path, true);
			}
		}
		if (path == null) {
			throw new NullPointerException();
		}
		return Pair.of(path, false);
	}

	/**
	 * Output the results of an analysis to stdout.
	 * 
	 * @param usertoken
	 * 
	 * @param 
	 */
	public void printResults(String usertoken,
			Pair<FunnelPath, Boolean> result) {
		FunnelPath path = result.getKey();
		Boolean value = result.getValue();
		System.out.println("Usertoken: " + usertoken.toString()
				+ "; path id: " + path.id 
				+ "; Finished: " + value.toString());
	}
}
