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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * The Class ParserTest.This class parses JSON Schema URI's from 
 * https://meta.wikimedia.org/wiki/Schema:*
 * 
 * The EventLogging extension stores it's metadata on a Wiki as a JSON schema.
 * This class provides methods to download such a metadata schema and retrieve
 * the properties fields and the associated types and stores this in a HashMap.
 */
public class Parser {

	/** The map. */
	private final HashMap<String, String> map = new HashMap<String, String>();

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
	public static void main(String args[]) throws JsonParseException, MalformedURLException, IOException {
		// This is an example
		Parser parser = new Parser();
		String jsonSchema = parser.loadEventLoggingJsonSchema("GettingStarted", "0");
		System.out.println(jsonSchema);
		parser.parseEventLoggingJsonSchem(jsonSchema);
	}

	/**
	 * Insert field.
	 *
	 * @param key the key
	 * @param entry a JsonNode object
	 */
	private void insertField(String key, Entry<String, JsonNode> entry) {
		Iterator<Entry<String, JsonNode>> it = entry.getValue().fields();
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

    public URL generateSchemaUrl(String schema, String revisionId) throws MalformedURLException {
        URL url = new URL("http://meta.wikimedia.org/w/index.php?action=raw&title=Schema:" + schema +"&oldid=" + revisionId);
        return url;
    }

	/**
	 * Load event logging json schema.
	 *
	 * @param schemaName the schema name
	 * @return a string containing the retrieved json schema.
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public String loadEventLoggingJsonSchema(String schemaName, String revisionId)
			throws MalformedURLException, IOException {
		URL url = generateSchemaUrl(schemaName, revisionId);
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
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory jf = mapper.getFactory();
		JsonParser jp = jf.createJsonParser(jsonSchema);
		JsonNode rootNode = mapper.readTree(jp);
		Iterator<Entry<String, JsonNode>> it = rootNode.fields();
		Entry<String, JsonNode> entry;
		String key;
		while (it.hasNext()) {
			entry = it.next();
			key = entry.getKey();
			insertField(key, entry);
		}
	}
}
