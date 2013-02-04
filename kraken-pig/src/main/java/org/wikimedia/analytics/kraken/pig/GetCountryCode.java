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
package org.wikimedia.analytics.kraken.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.maxmind.geoip.Country;
import com.maxmind.geoip.LookupService;

public class GetCountryCode extends EvalFunc<Tuple> {
	/** Wrapper class for getCountry and getCountryIpV6 */

	private static String ip4path;
	private static String ip6path;

	private LookupService ip4lookup;
	private LookupService ip6lookup;
	private TupleFactory tupleFactory = TupleFactory.getInstance();

	/**
	 * Constructs the UDF. Argument must contain the relative path of the GeoIP 
	 * database.
	 *
	 * @param ip4path the relative path to GeoIP.dat
	 */
	public GetCountryCode(String ip4path) {
		this(ip4path, null);
	}
	
	/**
	 * Constructs the UDF. Arguments must contain the relative paths to the
	 * GeoIPv4 and GeoIPv6 databases.
	 *
	 * @param ip4path the relative path to GeoIP.dat
	 * @param ip6path the relative path to GeoIPv6.dat
	 */
	public GetCountryCode(String ip4path, String ip6path) {
		GetCountryCode.ip4path = ip4path;
		GetCountryCode.ip6path = ip6path;
	}

	/** {@inheritDoc} */
	@Override
	public List<String> getCacheFiles() {
		List<String> cacheFiles = new ArrayList<String>(1);
		// Note that this forces us to use basenames only. If we need
		// to support other paths, we either need two arguments in the
		// constructor, or to parse the filename to extract the basename.
		cacheFiles.add(ip4path + "#" + ip4path);
		if (ip6path != null) {
			cacheFiles.add(ip6path + "#" + ip6path);
		}
		return cacheFiles;
	}

	/**
	 * {@inheritDoc}
	 *
	 * Returns a tuple containing the country code of the given ip address.
	 */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		Tuple output = null;
		if (input == null || input.size() == 0) {
			return null;
		}

		if (ip4lookup == null) {
			ip4lookup = new LookupService(ip4path);
		}
		
		if (ip6path != null && ip6lookup == null) {
			ip6lookup = new LookupService(ip6path);
		}

		String ip = (String) input.get(0);
		Country country = Pattern.matches("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$",ip) ? ip4lookup.getCountry(ip) : ip6lookup.getCountryV6(ip);

		if (country != null) {
			output = tupleFactory.newTuple(1);
			output.set(0, country.getCode());
		}
		
		if (country == null || country.getCode().equals("--")){
			warn("Couldn't get country for the ip: " + ip, PigWarning.UDF_WARNING_1);
		}

		return output;

	}

}
