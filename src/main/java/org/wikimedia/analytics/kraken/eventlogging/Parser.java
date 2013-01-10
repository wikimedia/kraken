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
package org.wikimedia.analytics.kraken.eventlogging;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * The Class Parser.This class parses JSON Schema URI's from 
 * https://meta.wikimedia.org/wiki/Schema:*
 * 
 * The EventLogging extension stores it's metadata on a Wiki as a JSON schema.
 * This class provides methods to download such a metadata schema and retrieve
 * the properties fields and the associated types and stores this in a HashMap.
 */
public class Parser {

	/** The map. */
	HashMap<String, String> map = new HashMap<String, String>();

	/**
	 * Instantiates a new parser.
	 */
	public Parser() {
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws JsonParseException the json parse exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void main(String args[]) throws JsonParseException, IOException {
		// This is an example
		Parser parser = new Parser();
		String jsonSchema = parser.loadEventLoggingJsonSchema("GettingStarted");
		System.out.println(jsonSchema);
		parser.parseEventLoggingJsonSchem(jsonSchema);
	}

	/**
	 * Insert field.
	 *
	 * @param key the key
	 * @param entry a JsonNode object
	 */
	public void insertField(String key, Entry<String, JsonNode> entry) {
		Iterator<Entry<String, JsonNode>> it = entry.getValue().getFields();
		Entry<String, JsonNode> prop;
		String type;
		while (it.hasNext()) {
			prop = it.next();
			if (prop.getKey().equals("type")) {
				type = prop.getValue().asText() ;
				if (type.equals("string")) {
					//Figure out whether it's a simple string or an enum
					JsonNode result = entry.getValue().path("enum");
					if (!result.isMissingNode()) {
						this.map.put(key, "arraylist");
					} else {
						this.map.put(key, type);
					}
				} else {
					this.map.put(key, type);
				}
			}
		}
	}

	/**
	 * Load event logging json schema.
	 *
	 * @param schemaName the schema name
	 * @return a string containing the retrieved json schema.
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public String loadEventLoggingJsonSchema(String schemaName)
			throws IOException {
		String urlStr = "http://meta.wikimedia.org/w/api.php?format=json&action=query&titles=Schema:"
				+ schemaName + "&prop=revisions&rvprop=content";
		URL url = new URL(urlStr);
		BufferedReader reader = null;
		StringBuilder buffer = new StringBuilder();
		try {
			reader = new BufferedReader(new InputStreamReader(url.openStream()));
			int cp;
			while ((cp = reader.read()) != -1) {
				buffer.append((char) cp);
			}
		} finally {
			if (reader != null)
				reader.close();
		}
		return buffer.toString();
	}

	/**
	 * Parses the event logging json schem.
	 *
	 * @param jsonSchema the json schema
	 * @throws JsonParseException the json parse exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void parseEventLoggingJsonSchem(String jsonSchema)
			throws JsonParseException, IOException {
		// There are definitively ways to do this recursive, but it's not worth
		// it; this code is not optimal but readable and easily fixable if
		// the eventLogging schema changes
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory jf = mapper.getJsonFactory();
		JsonParser jp = jf.createJsonParser(jsonSchema);
		JsonNode rootNode = mapper.readTree(jp);

		JsonNode overallSchema = rootNode.findParent("contentmodel");
		Iterator<Entry<String, JsonNode>> it = overallSchema.getFields();
		Entry<String, JsonNode> entry;
		String key;
		while (it.hasNext()) {
			entry = it.next();
			if ("*".equals(entry.getKey())) {
				JsonParser jp2 = jf.createJsonParser(entry.getValue()
						.getTextValue());
				JsonNode rootSchemaNode = mapper.readTree(jp2);
				JsonNode schema = rootSchemaNode.findParent("token");
				Iterator<Entry<String, JsonNode>> it2 = schema.getFields();
				while (it2.hasNext()) {
					entry = it2.next();
					key = entry.getKey();
					insertField(key, entry);
				}
				break;
			}
		}
	}
}
