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
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;

class Analysis {

	public Result run(String userToken, DirectedGraph<Node, DefaultEdge> history, Funnel funnel) {
		boolean bounced;

		ArrayList<FunnelPath> completedFunnelViaPaths = new ArrayList<FunnelPath>();
        for (FunnelPath path : funnel.paths) {
            List<Boolean> results = new ArrayList<Boolean>();
            boolean inFunnel = true;
            for (Pair<FunnelNode, FunnelNode> pair : path) {
                FunnelNode left = pair.getLeft();
                FunnelNode right = pair.getRight();

                inFunnel = hasArrivedAtNode(history, right) && inFunnel;
                bounced = hasBouncedFromFunnel(history, left, right);
                results.add(bounced);

                if (inFunnel && bounced) {
                    right.incrementBounced();
                } else if (inFunnel && left == null){
                    right.incrementImpression();
                } else if (inFunnel) {
                    left.incrementImpression();
                }

                }

            if (hasCompletedFunnel(results)) {
                completedFunnelViaPaths.add(path);
            }
        }
		return new Result(userToken, completedFunnelViaPaths);
	}

    private boolean hasArrivedAtNode(DirectedGraph<Node, DefaultEdge> history, FunnelNode targetVertex) {
        return history.containsVertex(targetVertex);
    }

    private boolean hasBouncedFromFunnel(
            DirectedGraph<Node, DefaultEdge> history,
            Node sourceVertex,
            Node targetVertex) {
        if (sourceVertex != null) {
            return !history.containsEdge(sourceVertex, targetVertex);
        } else {
            return history.vertexSet().size() > 1;
        }
    }

	private boolean hasCompletedFunnel(List<Boolean> results) {
        return results.contains(false);
	}

	/**
	 * Output the results of an analysis to stdout.
	 * @param result instance of the {@link Result} class
	 */
	public void printResults(Result result) {
		System.out.println(
            "User Token: " + result.userToken
            + "; Finished: " + result.getHasFinishedFunnel()
            + "; via " + result.completionPaths.size() + " paths"
        );
	}
}
