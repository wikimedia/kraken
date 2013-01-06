/**
 * Copyright 2012 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Modified for Kraken to initialize GeoIp database file in back-end.
 *
 * @version $Id: $Id
 */
package org.wikimedia.analytics.kraken.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
public class GeoIpLookup extends EvalFunc<Tuple> {

	private static final String EMPTY_STRING = "";

	private String ip4dat;
	private String ip6dat;

	private LookupService ip4lookup;
	private LookupService ip6lookup;
	private TupleFactory tupleFactory = TupleFactory.getInstance();

	
	private static final Map<String, String> countryCodeToContinentCode = new HashMap<String, String>() {
		private static final long serialVersionUID = 1L;

		/**
		 * HashMap that maps ISO_3166-1 country codes onto continent codes
		 */
		{
			put("AF", "AS");
			put("AX", "EU");
			put("AL", "EU");
			put("DZ", "AF");
			put("AS", "OC");
			put("AD", "EU");
			put("AO", "AF");
			put("AI", "NA");
			put("AQ", "AN");
			put("AG", "NA");
			put("AR", "SA");
			put("AM", "AS");
			put("AW", "NA");
			put("AU", "OC");
			put("AT", "EU");
			put("AZ", "AS");
			put("BS", "NA");
			put("BH", "AS");
			put("BD", "AS");
			put("BB", "NA");
			put("BY", "EU");
			put("BE", "EU");
			put("BZ", "NA");
			put("BJ", "AF");
			put("BM", "NA");
			put("BT", "AS");
			put("BO", "SA");
			put("BA", "EU");
			put("BW", "AF");
			put("BV", "AN");
			put("BR", "SA");
			put("IO", "AS");
			put("BN", "AS");
			put("BG", "EU");
			put("BF", "AF");
			put("BI", "AF");
			put("KH", "AS");
			put("CM", "AF");
			put("CA", "NA");
			put("CV", "AF");
			put("KY", "NA");
			put("CF", "AF");
			put("TD", "AF");
			put("CL", "SA");
			put("CN", "AS");
			put("CX", "AS");
			put("CC", "AS");
			put("CO", "SA");
			put("KM", "AF");
			put("CD", "AF");
			put("CG", "AF");
			put("CK", "OC");
			put("CR", "NA");
			put("CI", "AF");
			put("HR", "EU");
			put("CU", "NA");
			put("CY", "AS");
			put("CZ", "EU");
			put("DK", "EU");
			put("DJ", "AF");
			put("DM", "NA");
			put("DO", "NA");
			put("EC", "SA");
			put("EG", "AF");
			put("SV", "NA");
			put("GQ", "AF");
			put("ER", "AF");
			put("EE", "EU");
			put("ET", "AF");
			put("FO", "EU");
			put("FK", "SA");
			put("FJ", "OC");
			put("FI", "EU");
			put("FR", "EU");
			put("GF", "SA");
			put("PF", "OC");
			put("TF", "AN");
			put("GA", "AF");
			put("GM", "AF");
			put("GE", "AS");
			put("DE", "EU");
			put("GH", "AF");
			put("GI", "EU");
			put("GR", "EU");
			put("GL", "NA");
			put("GD", "NA");
			put("GP", "NA");
			put("GU", "OC");
			put("GT", "NA");
			put("GG", "EU");
			put("GN", "AF");
			put("GW", "AF");
			put("GY", "SA");
			put("HT", "NA");
			put("HM", "AN");
			put("VA", "EU");
			put("HN", "NA");
			put("HK", "AS");
			put("HU", "EU");
			put("IS", "EU");
			put("IN", "AS");
			put("ID", "AS");
			put("IR", "AS");
			put("IQ", "AS");
			put("IE", "EU");
			put("IM", "EU");
			put("IL", "AS");
			put("IT", "EU");
			put("JM", "NA");
			put("JP", "AS");
			put("JE", "EU");
			put("JO", "AS");
			put("KZ", "AS");
			put("KE", "AF");
			put("KI", "OC");
			put("KP", "AS");
			put("KR", "AS");
			put("KW", "AS");
			put("KG", "AS");
			put("LA", "AS");
			put("LV", "EU");
			put("LB", "AS");
			put("LS", "AF");
			put("LR", "AF");
			put("LY", "AF");
			put("LI", "EU");
			put("LT", "EU");
			put("LU", "EU");
			put("MO", "AS");
			put("MK", "EU");
			put("MG", "AF");
			put("MW", "AF");
			put("MY", "AS");
			put("MV", "AS");
			put("ML", "AF");
			put("MT", "EU");
			put("MH", "OC");
			put("MQ", "NA");
			put("MR", "AF");
			put("MU", "AF");
			put("YT", "AF");
			put("MX", "NA");
			put("FM", "OC");
			put("MD", "EU");
			put("MC", "EU");
			put("MN", "AS");
			put("ME", "EU");
			put("MS", "NA");
			put("MA", "AF");
			put("MZ", "AF");
			put("MM", "AS");
			put("NA", "AF");
			put("NR", "OC");
			put("NP", "AS");
			put("AN", "NA");
			put("NL", "EU");
			put("NC", "OC");
			put("NZ", "OC");
			put("NI", "NA");
			put("NE", "AF");
			put("NG", "AF");
			put("NU", "OC");
			put("NF", "OC");
			put("MP", "OC");
			put("NO", "EU");
			put("OM", "AS");
			put("PK", "AS");
			put("PW", "OC");
			put("PS", "AS");
			put("PA", "NA");
			put("PG", "OC");
			put("PY", "SA");
			put("PE", "SA");
			put("PH", "AS");
			put("PN", "OC");
			put("PL", "EU");
			put("PT", "EU");
			put("PR", "NA");
			put("QA", "AS");
			put("RE", "AF");
			put("RO", "EU");
			put("RU", "EU");
			put("RW", "AF");
			put("SH", "AF");
			put("KN", "NA");
			put("LC", "NA");
			put("PM", "NA");
			put("VC", "NA");
			put("WS", "OC");
			put("SM", "EU");
			put("ST", "AF");
			put("SA", "AS");
			put("SN", "AF");
			put("RS", "EU");
			put("SC", "AF");
			put("SL", "AF");
			put("SG", "AS");
			put("SK", "EU");
			put("SI", "EU");
			put("SB", "OC");
			put("SO", "AF");
			put("ZA", "AF");
			put("GS", "AN");
			put("ES", "EU");
			put("LK", "AS");
			put("SD", "AF");
			put("SR", "SA");
			put("SJ", "EU");
			put("SZ", "AF");
			put("SE", "EU");
			put("CH", "EU");
			put("SY", "AS");
			put("TW", "AS");
			put("TJ", "AS");
			put("TZ", "AF");
			put("TH", "AS");
			put("TL", "AS");
			put("TG", "AF");
			put("TK", "OC");
			put("TO", "OC");
			put("TT", "NA");
			put("TN", "AF");
			put("TR", "AS");
			put("TM", "AS");
			put("TC", "NA");
			put("TV", "OC");
			put("UG", "AF");
			put("UA", "EU");
			put("AE", "AS");
			put("GB", "EU");
			put("UM", "OC");
			put("US", "NA");
			put("UY", "SA");
			put("UZ", "AS");
			put("VU", "OC");
			put("VE", "SA");
			put("VN", "AS");
			put("VG", "NA");
			put("VI", "NA");
			put("WF", "OC");
			put("EH", "AF");
			put("YE", "AS");
			put("ZM", "AF");
			put("ZW", "AF");
		}};

		// HashMap that maps ISO_3166-1 continent codes to Continent Names
		private static final Map<String, String> continentCodeToContinentName = new HashMap<String, String>() {/**
		 * 
		 */
			private static final long serialVersionUID = 1L;

			{
				put("AS", "Asia");
				put("AN", "Antarctica");
				put("AF", "Africa");
				put("SA", "South America");
				put("EU", "Europe");
				put("OC", "Oceania");
				put("NA", "North America");
			}};


			/**
			 * Create a IP -> Location mapper.
			 * Pig example:
			 *   DEFINE GeoIpLookup com.mozilla.pig.eval.geoip.GeoIpLookup('GeoIPCity.dat');
			 *   foo = LOAD ...
			 *   bar = foreach foo generate GeoIpLookup(ip_address) AS
			 *           location:tuple(country:chararray, country_code:chararray,
			 *             region:chararray, city:chararray,
			 *             postal_code:chararray, metro_code:int);
			 *
			 * This will expect a file in hdfs in /user/you/GeoIPCity.dat
			 *
			 * Using the getCacheFiles approach, you no longer need to specify the
			 *  -Dmapred.cache.archives
			 *  -Dmapred.create.symlink
			 * options to pig.
			 *
			 * @param ip4dat Basename of the GeoIPCity Database file.  Should be located in your home dir in HDFS
			 * @throws IOException if any.
			 */
			public GeoIpLookup(String ip4dat) {
				this(ip4dat, null);
			}

			/**
			 * <p>Constructor for GeoIpLookup.</p>
			 *
			 * @param ip4dat a {@link java.lang.String} object.
			 * @param ip6dat a {@link java.lang.String} object.
			 */
			public GeoIpLookup(String ip4dat, String ip6dat) {
				this.ip4dat=ip4dat;
				this.ip6dat=ip6dat;
			}

			/** {@inheritDoc} */
			@Override
			public List<String> getCacheFiles() {
				List<String> cacheFiles = new ArrayList<String>(1);
				// Note that this forces us to use basenames only.  If we need
				// to support other paths, we either need two arguments in the
				// constructor, or to parse the filename to extract the basename.
				cacheFiles.add(ip4dat + "#" + ip4dat);
				if (ip6dat != null) {
					cacheFiles.add(ip6dat + "#" + ip6dat);
				}
				return cacheFiles;
			}

			/** {@inheritDoc} */
			@Override
			public Tuple exec(Tuple input) throws IOException {
				if (input == null || input.size() == 0) {
					return null;
				}

				if (ip4lookup == null) {
					ip4lookup = new LookupService(ip4dat);
				}

				if (ip6dat != null && ip6lookup == null) {
					ip6lookup = new LookupService(ip6dat);
				}

				String ip = (String)input.get(0);
				Location location = ip4lookup.getLocation(ip);
				if (location != null) {
					// get the continent code and name from the
					// static mappings defined at the top of this class.
					String continentCode = location.countryCode != null ? countryCodeToContinentCode.get(location.countryCode) : EMPTY_STRING;
					String continentName = continentCode != EMPTY_STRING ? continentCodeToContinentName.get(continentCode) : EMPTY_STRING;

					Tuple output = tupleFactory.newTuple(8);
					output.set(0, location.countryName != null ? location.countryName : EMPTY_STRING);
					output.set(1, location.countryCode != null ? location.countryCode : EMPTY_STRING);
					output.set(2, location.region != null ? location.region : EMPTY_STRING);
					output.set(3, location.city != null ? location.city : EMPTY_STRING);
					output.set(4, location.postalCode != null ? location.postalCode : EMPTY_STRING);
					output.set(5, location.metro_code);
					output.set(6, continentCode);
					output.set(7, continentName);
					return output;
				}

				warn("getLocation() returned null on input: " + ip, PigWarning.UDF_WARNING_1);
				return null;
			}

}
