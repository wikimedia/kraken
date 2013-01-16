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

import org.apache.commons.lang3.tuple.Pair;

public class FunnelPath implements Iterable<Pair<Node, Node>> {

	public final int id;
	private int currentSize;
	public final ArrayList<Node> nodes = new ArrayList<Node>();

	public FunnelPath(int id) {
		this.id = id;
		setCurrentSize();
	}

	private void setCurrentSize() {
		this.currentSize = nodes.size();
	}

	public Iterator<Pair<Node, Node>> iterator() {
		setCurrentSize();
        return new Iterator<Pair<Node, Node>>() {

            private int currentIndex = -1;

            public boolean hasNext() {
                return currentIndex < currentSize -1
                        && nodes.toArray()[currentIndex + 1] != null;
            }

            public Pair<Node, Node> next() {
                //TODO: this is a counter-intuitive solution so this needs to be
                //refactored. The problem is this, the potential referral nodes
                //to the starting point of a funnel are endless and so to make
                //sure that a visitor actually entered the funnel we just want
                //to check whether the start node has been reached. The purpose
                //of this iterator is however to determine whether, once inside
                //a funnel, a visitor goes from A to B. The Node L = null hack
                //makes it possible to have a single iterator that can be used
                //both to determine whether a funnel was started and determine
                //when/whether a visitor dropped out of the funnel.
                //Obviously code calling this iterator needs to handle null.
                Node L = null;
                if (currentIndex > -1) {
                    L = nodes.get(currentIndex);
                }
                Node R = nodes.get(currentIndex + 1);
                currentIndex++;
                return Pair.of(L, R);
            }

            public void remove() {
                //
            }
        };
	}

}
