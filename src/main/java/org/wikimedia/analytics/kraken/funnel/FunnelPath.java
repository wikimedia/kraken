package org.wikimedia.analytics.kraken.funnel;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;

public class FunnelPath implements Iterable<Pair<Node, Node>> {

	public String id;
	private int currentSize;
	public ArrayList<Node> nodes = new ArrayList<Node>();

	public FunnelPath(String id) {
		this.id = id;
		setCurrentSize();
	}

	private void setCurrentSize() {
		this.currentSize = nodes.size();
	}

	public Iterator<Pair<Node, Node>> iterator() {
		setCurrentSize();
		Iterator<Pair<Node, Node>> it = new Iterator<Pair<Node, Node>>() {

			private int currentIndex = 0;

			public boolean hasNext() {
				return currentIndex < currentSize + 1
						&& nodes.toArray()[currentIndex] != null;
			}

			public Pair<Node, Node> next() {
				Node L = nodes.get(currentSize);
				Node R = nodes.get(currentSize + 1);
				currentSize++;
				return Pair.of(L, R);
			}

			public void remove() {
				throw new NotImplementedException();
			}
		};
		return it;
	}

}
