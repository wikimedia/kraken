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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.wikimedia.analytics.kraken.exceptions.MalformedFunnelException;

/*
 * A FunnelNode is a building block for a Funnel. The simplest funnel is:
 * A -> B where both A and B are instances of the FunnelNode class. To construct
 * a FunnelNode supply it a list of semi-colon separated edges, the edges are 
 * separated by a comma.  
 */
public class FunnelNode extends Node{
	/** The nodeDefinition. */
	private final Map<ComponentType, Pattern> nodeDefinition = new HashMap<ComponentType, Pattern>();
	public Integer impression = 0;


	/**
	 * Instantiates a new node. 
	 *
	 * @param edge contains a step in a funnel.
	 * @throws MalformedFunnelException 
	 */
	public FunnelNode(String edge) throws MalformedFunnelException, PatternSyntaxException {
		String[] nodes = edge.split("=");
		ComponentType key;
		System.out.println(Arrays.toString(nodes));
		try {
			key = ComponentType.valueOf(nodes[0].toUpperCase());
		} catch (IllegalArgumentException e) {
			key = null;
		}
		Pattern value = Pattern.compile(nodes[1], Pattern.CASE_INSENSITIVE);
		if (key != null) {
			this.nodeDefinition.put(key, value);
		}
	}

    /**
     * Whether a UserActionNode matches this FunnelNode
     *
     * @param {@link UserActionNode} instance to determine whether that
     *         action was taken inside/outside of a funnel.
     * @return true if the regular expression of {@link FunnelNode} matches
     * the node definition of {@link UserActionNode} otherwise false.
     */
    public boolean matches(UserActionNode node){
        boolean match = true;
        for (ComponentType key : this.nodeDefinition.keySet()){
            match = match
                 && node.componentValues.containsKey(key)
                 && this.nodeDefinition.get(key).matcher(node.componentValues.get(key)).matches();
        }
        return match;
    }

	public boolean equals(Object obj) {
		if (obj == null) { return false; }
        if (this == obj) { return true; }
        if (obj instanceof UserActionNode) { return this.matches((UserActionNode)obj); }
        if (!(obj instanceof FunnelNode)) { return false; }

		FunnelNode node = (FunnelNode) obj;
		return new EqualsBuilder().
				append(this.nodeDefinition.toString(), node.nodeDefinition.toString()).
				isEquals();
	}

	public int hashCode() {
		return new HashCodeBuilder().append(this.toString()).toHashCode();
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(100);
		int e = 1;
		for (ComponentType key : ComponentType.values()) {
			Pattern value = this.nodeDefinition.get(key);
			if (value != null) {
				sb.append(value.toString());
			}
			if (nodeDefinition.size() > 1 && e < nodeDefinition.size()) {
				sb.append(":");
			}
			e++;
		}
		return sb.toString();
	}

}
