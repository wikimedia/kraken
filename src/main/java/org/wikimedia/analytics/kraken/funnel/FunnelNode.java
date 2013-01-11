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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;

/*
 * A FunnelNode is a building block for a Funnel. The simplest funnel is:
 * A -> B where both A and B are instances of the FunnelNode class. To construct
 * a FunnelNode supply it a list of semi-colon separated edges, the edges are 
 * separated by a comma.  
 */
public class FunnelNode extends Node{
	/** The nodeDefinition. */
	public Map<ComponentType, Pattern> nodeDefinition = new HashMap<ComponentType, Pattern>();

	/**
	 * Instantiates a new node. 
	 *
	 * @param funnelDefinition
	 * @throws MalformedFunnelException 
	 */
	public FunnelNode(String edge) throws MalformedFunnelException, PatternSyntaxException {
		String[] nodes = edge.split("=");
		//		if (pairs.length != ComponentType.values().length) {
		//			throw new MalformedFunnelException("Each node should contain the " +
		//					"following components (separated by a colon): " + Arrays.toString(ComponentType.values()));
		//		}
		ComponentType key;
		System.out.println(Arrays.toString(nodes));
		try {
			key = ComponentType.valueOf(nodes[0]);
		} catch (IllegalArgumentException e) {
			key = null;
		}
		Pattern value = Pattern.compile(nodes[1], Pattern.CASE_INSENSITIVE);
		if (key != null) {
			this.nodeDefinition.put(key, value);
		}
	}

public boolean equals(FunnelNode b) {
	if (this == b) return true;
	if (!(b instanceof FunnelNode)) return false;
	FunnelNode node = (FunnelNode)b;
	if (this.toString().equals(node.toString())) {
		return true;
	} else {
		return false;
	}
}

public String toString() {
	StringBuilder sb = new StringBuilder(100);
	int e = 1;
	for (ComponentType key : ComponentType.values()) {
		Pattern value = this.nodeDefinition.get(key);
		if (value != null) {
			sb.append(value.toString());
		} else {
			sb.append(".");
		}
		if (e != nodeDefinition.size()) {
			sb.append(":");
		}
		e++;
	}
	return sb.toString();
}

}
