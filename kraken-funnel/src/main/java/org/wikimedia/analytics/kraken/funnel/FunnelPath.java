/**
 *Copyright (C) 2012-2013  Wikimedia Foundation
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

 */

package org.wikimedia.analytics.kraken.funnel;

import java.util.ArrayList;
import java.util.Iterator;

import com.google.gson.JsonObject;
import org.apache.commons.lang3.tuple.Pair;
import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;

public class FunnelPath implements Iterable<Pair<FunnelNode, FunnelNode>> {

	public final int id;
	private int currentSize;
	public final ArrayList<FunnelNode> nodes;
    private FunnelNode Left;
    private FunnelNode Right;

	public FunnelPath(int id) {
		this.id = id;
        nodes = new ArrayList<FunnelNode>();
        setCurrentSize();

    }

	private void setCurrentSize() {
		this.currentSize = this.nodes.size();
	}

	public Iterator<Pair<FunnelNode, FunnelNode>> iterator() {
		setCurrentSize();
        return new Iterator<Pair<FunnelNode, FunnelNode>>() {

            private int currentIndex = 0;

            public boolean hasNext() {
                return currentIndex < currentSize -1
                        && nodes.toArray()[currentIndex] != null;
            }

            public Pair<FunnelNode, FunnelNode> next() {
                Left = nodes.get(currentIndex);
                Right = nodes.get(currentIndex + 1);
                currentIndex++;
                return Pair.of(Left, Right);
            }

            public void remove() {
               //
            }
        };
	}

}
