/**
Copyright (C) 2012  Wikimedia Foundation

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. 
 */

package org.wikimedia.analytics.kraken.pig;

import java.io.IOException;
import org.apache.pig.data.*;

public class isValidIPv6Address extends RegexMatch {

	/**
	 * Constructs the UDF.
	 */
	public isValidIPv6Address() {
		super(
				"^(?:[0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}(?::(?:[0-9]{1,3}\\.){3}[0-9]{1,3})*$");
	}

	/**
	 * Aeturns true if the ip address is IPv6
	 * @param input  an ip address.
	 */
	public Boolean exec(Tuple input) throws IOException {
		return super.exec(input);
	}

}