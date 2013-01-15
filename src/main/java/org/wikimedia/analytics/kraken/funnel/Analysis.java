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
		if (sourceVertex != null) {
			return !history.containsEdge(sourceVertex, targetVertex);
		} else {
			return !history.containsVertex(targetVertex);
		}
	}

	private void incrementImpression(FunnelPath path, Node node) {
		FunnelNode funnelNode = (FunnelNode) path.nodes.get(0);
		funnelNode.impression++;
	}

	public Result run(String userToken, DirectedGraph<Node, DefaultEdge> history,
			Funnel funnel) {
		FunnelPath path = null;
		Boolean bounced;
		Boolean completedFunnel = false;
		Iterator<FunnelPath> fp = funnel.paths.listIterator();
		while (fp.hasNext()) {
			Pair<Node, Node> pair;
			List<Boolean> results = new ArrayList<Boolean>();
			path = fp.next();
			Iterator<Pair<Node, Node>> it = path.iterator();
			while (it.hasNext()) {
				pair = it.next();
				bounced = (hasBouncedFromFunnel(history, pair.getLeft(),
						pair.getRight()));
				if (!bounced) {
					incrementImpression(path, pair.getRight());
				}
			results.add(bounced);
			completedFunnel =hasCompletedFunnel(results); 
			}
		}
		return new Result(userToken, path.id, completedFunnel);
	}

	public boolean hasCompletedFunnel(List<Boolean> results) {
		if (!results.contains(false)) {
			return true;
		} else
			return false;
	}

	/**
	 * Output the results of an analysis to stdout.
	 * 
	 * @param usertoken
	 * 
	 * @param 
	 */
	public void printResults(String usertoken,
			Result result) {
		System.out.println("Usertoken: " + result.userToken
				+ "; path id: " + result.funnelPathId 
				+ "; Finished: " + result.hasFinishedFunnel);
	}
}
