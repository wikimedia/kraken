/**
Method exec() takes a tuple containing a Wikimedia URL and returns a tuple
	containing (if possible) it's language, boolean on whether it's a mobile site, 
	and it's domain name.

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

/*
 * In these comments I may refer to any part of the host separated by '.' a subdomain
 * 	for example: in en.m.wikipedia.org, the subdomains are 'en', 'm', 'wikipedia'.
 */

package org.wikimedia.analytics.kraken;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class ParseWikiUrl extends EvalFunc<Tuple> {
	private static Set<String> languages;
	private static String languageFile;
	private static Tuple defaultOutput;
	
	public ParseWikiUrl() {
		languageFile = "resources/languages.txt";
	}
	
	public ParseWikiUrl(String languageFile) {
		ParseWikiUrl.languageFile = languageFile;
	}
	
	public Tuple exec(Tuple input) throws ExecException {		
		String language = "N/A";
		Boolean isMobile = null;
		String domain = "N/A";
		
		if(input == null) {
			warn("null input", PigWarning.UDF_WARNING_1);
			return defaultOutput;
		}
		
		if(defaultOutput == null) {
			defaultOutput = TupleFactory.getInstance().newTuple(3);
			defaultOutput.set(0, language);
			defaultOutput.set(1, isMobile);
			defaultOutput.set(2, domain);
		}
		
		//gets the urlString from the first argument, return if 
		String urlString;
		
		try {
			urlString = (String)input.get(0);
		} catch (Exception e) {
			warn("argument is invalid", PigWarning.UDF_WARNING_1);
			return defaultOutput;
		}
		
		if(urlString == null) {
			warn("null input", PigWarning.UDF_WARNING_1);
			return defaultOutput;
		}
		
		
		//use url class for parsing
		URL url;	
		try {
			url = new URL(urlString);
		} catch (MalformedURLException e) {
			warn("malformed URL: " + urlString, PigWarning.UDF_WARNING_1);
			return defaultOutput;
		}	
		
		//gets the host
		String host = url.getHost();
		String[] subdomains = host.split("\\.");
		

		//if subdomains has less than two elements then can't find domain so return
		if(subdomains.length < 2) {
			warn("host name: " + host + " has less than two subdomains", PigWarning.UDF_WARNING_1);
			return defaultOutput;
		}
		
		//add language codes from languageFile to a hash set
		if(languages==null) {
			languages = new HashSet<String>();
			try {
				BufferedReader inputStream = new BufferedReader(new FileReader(languageFile));
				for(String s = inputStream.readLine(); s !=null; s = inputStream.readLine()) {
					languages.add(s);
				}
				inputStream.close();
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}

		//takes the first subdomain and check if it is a language code
		String firstSubDomain = subdomains[0];
		if(languages.contains(firstSubDomain)) {
			language = firstSubDomain;
		}
		
		//sets the domain as the second to the last subdomain concatenated with the last
		domain = subdomains[subdomains.length - 2] + "." + subdomains[subdomains.length - 1].substring(0,3); 
		
		//default isMobile to false since the domain is fine
		isMobile = false;
		
		//iterate through each subdomain from the 2nd subdomain to the 3rd to the last subdomain
		//	to look for 'm'
		for(int i = 1; i < subdomains.length-2 ; i++) {;
			//if this subdomain is 'm' set isMobile flag to true
			if(subdomains[i].equals("m")) {
				isMobile = true;
				break;
			}
		}
		
		//create the tuple for output
		Tuple output = TupleFactory.getInstance().newTuple(3);
		output.set(0, language);
		output.set(1, isMobile.toString());
		output.set(2, domain);
		return output;
	}
	
	public Schema outputSchema(Schema input) {
		Schema inputModel = new Schema(new FieldSchema(null, DataType.CHARARRAY));
		if (!Schema.equals(inputModel, input, true, true)) {
			String msg = "";
			throw new IllegalArgumentException("Expected input schema "
					+ inputModel + ", received schema " + input + msg);
		}
		List<FieldSchema> tupleFields = new LinkedList<FieldSchema>();
		tupleFields.add(new FieldSchema("language", DataType.CHARARRAY));
		tupleFields.add(new FieldSchema("isMobile", DataType.CHARARRAY));
		tupleFields.add(new FieldSchema("domain", DataType.CHARARRAY));
		Schema tupleSchema = new Schema(tupleFields);
		try {
			return new Schema(new FieldSchema(null, tupleSchema, DataType.TUPLE));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}

}
