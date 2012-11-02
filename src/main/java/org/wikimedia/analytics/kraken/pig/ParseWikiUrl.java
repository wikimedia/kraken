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

package org.wikimedia.analytics.kraken.pig;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
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
	private static Tuple defaultOutput;
	private static boolean useBoolean;
	
	public ParseWikiUrl() {
		this("src/main/resources/languages.txt");
	}
	
	public ParseWikiUrl(boolean useBoolean) {
		this("src/main/resources/languages.txt", useBoolean);
	}
	
	public ParseWikiUrl(String languageFile) {
		this(languageFile, false);
	}
	
	public ParseWikiUrl(String languageFile, boolean useBoolean) {
		ParseWikiUrl.useBoolean = useBoolean;
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
			defaultOutput.set(1, null);
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
			languages.add("en");
			languages.add("de");
			languages.add("fr");
			languages.add("nl");
			languages.add("it");
			languages.add("es");
			languages.add("pl");
			languages.add("ru");
			languages.add("ja");
			languages.add("pt");
			languages.add("zh");
			languages.add("sv");
			languages.add("vi");
			languages.add("uk");
			languages.add("ca");
			languages.add("no");
			languages.add("fi");
			languages.add("cs");
			languages.add("fa");
			languages.add("hu");
			languages.add("ro");
			languages.add("ko");
			languages.add("ar");
			languages.add("tr");
			languages.add("id");
			languages.add("sk");
			languages.add("eo");
			languages.add("da");
			languages.add("sr");
			languages.add("kk");
			languages.add("lt");
			languages.add("eu");
			languages.add("ms");
			languages.add("he");
			languages.add("bg");
			languages.add("sl");
			languages.add("vo");
			languages.add("hr");
			languages.add("war");
			languages.add("hi");
			languages.add("et");
			languages.add("gl");
			languages.add("az");
			languages.add("nn");
			languages.add("simple");
			languages.add("la");
			languages.add("el");
			languages.add("th");
			languages.add("sh");
			languages.add("oc");
			languages.add("new");
			languages.add("mk");
			languages.add("roa-rup");
			languages.add("ka");
			languages.add("tl");
			languages.add("pms");
			languages.add("ht");
			languages.add("be");
			languages.add("te");
			languages.add("ta");
			languages.add("be-x-old");
			languages.add("uz");
			languages.add("lv");
			languages.add("br");
			languages.add("ceb");
			languages.add("sq");
			languages.add("jv");
			languages.add("mg");
			languages.add("mr");
			languages.add("cy");
			languages.add("lb");
			languages.add("is");
			languages.add("bs");
			languages.add("hy");
			languages.add("my");
			languages.add("yo");
			languages.add("an");
			languages.add("lmo");
			languages.add("ml");
			languages.add("pnb");
			languages.add("fy");
			languages.add("bpy");
			languages.add("af");
			languages.add("bn");
			languages.add("sw");
			languages.add("io");
			languages.add("ne");
			languages.add("gu");
			languages.add("zh-yue");
			languages.add("nds");
			languages.add("ur");
			languages.add("ba");
			languages.add("scn");
			languages.add("ku");
			languages.add("ast");
			languages.add("qu");
			languages.add("su");
			languages.add("diq");
			languages.add("tt");
			languages.add("ga");
			languages.add("ky");
			languages.add("cv");
			languages.add("ia");
			languages.add("nap");
			languages.add("bat-smg");
			languages.add("map-bms");
			languages.add("als");
			languages.add("wa");
			languages.add("kn");
			languages.add("am");
			languages.add("gd");
			languages.add("ckb");
			languages.add("sco");
			languages.add("bug");
			languages.add("tg");
			languages.add("mzn");
			languages.add("zh-min-nan");
			languages.add("yi");
			languages.add("vec");
			languages.add("arz");
			languages.add("hif");
			languages.add("roa-tara");
			languages.add("nah");
			languages.add("os");
			languages.add("sah");
			languages.add("mn");
			languages.add("sa");
			languages.add("pam");
			languages.add("hsb");
			languages.add("li");
			languages.add("mi");
			languages.add("si");
			languages.add("se");
			languages.add("co");
			languages.add("gan");
			languages.add("glk");
			languages.add("bar");
			languages.add("fo");
			languages.add("ilo");
			languages.add("bo");
			languages.add("bcl");
			languages.add("mrj");
			languages.add("fiu-vro");
			languages.add("nds-nl");
			languages.add("ps");
			languages.add("tk");
			languages.add("vls");
			languages.add("gv");
			languages.add("rue");
			languages.add("pa");
			languages.add("dv");
			languages.add("xmf");
			languages.add("pag");
			languages.add("nrm");
			languages.add("kv");
			languages.add("zea");
			languages.add("koi");
			languages.add("km");
			languages.add("rm");
			languages.add("csb");
			languages.add("lad");
			languages.add("udm");
			languages.add("or");
			languages.add("mhr");
			languages.add("mt");
			languages.add("fur");
			languages.add("lij");
			languages.add("wuu");
			languages.add("ug");
			languages.add("pi");
			languages.add("sc");
			languages.add("zh-classical");
			languages.add("frr");
			languages.add("bh");
			languages.add("nov");
			languages.add("ksh");
			languages.add("ang");
			languages.add("so");
			languages.add("stq");
			languages.add("kw");
			languages.add("nv");
			languages.add("hak");
			languages.add("ay");
			languages.add("frp");
			languages.add("vep");
			languages.add("ext");
			languages.add("pcd");
			languages.add("szl");
			languages.add("gag");
			languages.add("gn");
			languages.add("ie");
			languages.add("ln");
			languages.add("haw");
			languages.add("xal");
			languages.add("eml");
			languages.add("pfl");
			languages.add("pdc");
			languages.add("rw");
			languages.add("krc");
			languages.add("crh");
			languages.add("ace");
			languages.add("to");
			languages.add("as");
			languages.add("ce");
			languages.add("kl");
			languages.add("arc");
			languages.add("dsb");
			languages.add("myv");
			languages.add("bjn");
			languages.add("pap");
			languages.add("sn");
			languages.add("tpi");
			languages.add("lbe");
			languages.add("mdf");
			languages.add("wo");
			languages.add("kab");
			languages.add("jbo");
			languages.add("av");
			languages.add("lez");
			languages.add("srn");
			languages.add("cbk-zam");
			languages.add("ty");
			languages.add("bxr");
			languages.add("lo");
			languages.add("kbd");
			languages.add("ab");
			languages.add("tet");
			languages.add("mwl");
			languages.add("ltg");
			languages.add("na");
			languages.add("ig");
			languages.add("kg");
			languages.add("nso");
			languages.add("za");
			languages.add("kaa");
			languages.add("zu");
			languages.add("rmy");
			languages.add("chy");
			languages.add("cu");
			languages.add("tn");
			languages.add("chr");
			languages.add("got");
			languages.add("cdo");
			languages.add("sm");
			languages.add("bi");
			languages.add("mo");
			languages.add("bm");
			languages.add("iu");
			languages.add("pih");
			languages.add("ss");
			languages.add("sd");
			languages.add("pnt");
			languages.add("ee");
			languages.add("om");
			languages.add("ha");
			languages.add("ki");
			languages.add("ti");
			languages.add("ts");
			languages.add("ks");
			languages.add("sg");
			languages.add("ve");
			languages.add("rn");
			languages.add("cr");
			languages.add("ak");
			languages.add("lg");
			languages.add("tum");
			languages.add("dz");
			languages.add("ny");
			languages.add("ik");
			languages.add("ff");
			languages.add("ch");
			languages.add("st");
			languages.add("fj");
			languages.add("tw");
			languages.add("xh");
			languages.add("ng");
			languages.add("ii");
			languages.add("cho");
			languages.add("mh");
			languages.add("aa");
			languages.add("kj");
			languages.add("ho");
			languages.add("mus");
			languages.add("kr");
			languages.add("hz");
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
		output.set(1, useBoolean ? isMobile : isMobile.toString());
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
