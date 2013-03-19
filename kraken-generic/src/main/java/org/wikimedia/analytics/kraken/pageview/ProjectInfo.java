package org.wikimedia.analytics.kraken.pageview;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

import java.nio.charset.Charset;
import java.util.*;

/**
 * Processes a hostname to extract WMF Project info.
 */
public class ProjectInfo {
    private static Map<String, String> SITE_VERSIONS = new HashMap<String, String>();
    private static Set<String> LANGUAGES = new HashSet<String>();


    static {
        SITE_VERSIONS.put("zero",   "Z");
        SITE_VERSIONS.put("m",      "M");
        SITE_VERSIONS.put("mobile", "M");

        // List<String> langs = Resources.readLines(Resources.getResource("languages.txt"), Charset.defaultCharset());
        LANGUAGES.add("en");
        LANGUAGES.add("de");
        LANGUAGES.add("fr");
        LANGUAGES.add("nl");
        LANGUAGES.add("it");
        LANGUAGES.add("es");
        LANGUAGES.add("pl");
        LANGUAGES.add("ru");
        LANGUAGES.add("ja");
        LANGUAGES.add("pt");
        LANGUAGES.add("zh");
        LANGUAGES.add("sv");
        LANGUAGES.add("vi");
        LANGUAGES.add("uk");
        LANGUAGES.add("ca");
        LANGUAGES.add("no");
        LANGUAGES.add("fi");
        LANGUAGES.add("cs");
        LANGUAGES.add("fa");
        LANGUAGES.add("hu");
        LANGUAGES.add("ro");
        LANGUAGES.add("ko");
        LANGUAGES.add("ar");
        LANGUAGES.add("tr");
        LANGUAGES.add("id");
        LANGUAGES.add("sk");
        LANGUAGES.add("eo");
        LANGUAGES.add("da");
        LANGUAGES.add("sr");
        LANGUAGES.add("kk");
        LANGUAGES.add("lt");
        LANGUAGES.add("eu");
        LANGUAGES.add("ms");
        LANGUAGES.add("he");
        LANGUAGES.add("bg");
        LANGUAGES.add("sl");
        LANGUAGES.add("vo");
        LANGUAGES.add("hr");
        LANGUAGES.add("war");
        LANGUAGES.add("hi");
        LANGUAGES.add("et");
        LANGUAGES.add("gl");
        LANGUAGES.add("az");
        LANGUAGES.add("nn");
        LANGUAGES.add("simple");
        LANGUAGES.add("la");
        LANGUAGES.add("el");
        LANGUAGES.add("th");
        LANGUAGES.add("sh");
        LANGUAGES.add("oc");
        LANGUAGES.add("new");
        LANGUAGES.add("mk");
        LANGUAGES.add("roa-rup");
        LANGUAGES.add("ka");
        LANGUAGES.add("tl");
        LANGUAGES.add("pms");
        LANGUAGES.add("ht");
        LANGUAGES.add("be");
        LANGUAGES.add("te");
        LANGUAGES.add("ta");
        LANGUAGES.add("be-x-old");
        LANGUAGES.add("uz");
        LANGUAGES.add("lv");
        LANGUAGES.add("br");
        LANGUAGES.add("ceb");
        LANGUAGES.add("sq");
        LANGUAGES.add("jv");
        LANGUAGES.add("mg");
        LANGUAGES.add("mr");
        LANGUAGES.add("cy");
        LANGUAGES.add("lb");
        LANGUAGES.add("is");
        LANGUAGES.add("bs");
        LANGUAGES.add("hy");
        LANGUAGES.add("my");
        LANGUAGES.add("yo");
        LANGUAGES.add("an");
        LANGUAGES.add("lmo");
        LANGUAGES.add("ml");
        LANGUAGES.add("pnb");
        LANGUAGES.add("fy");
        LANGUAGES.add("bpy");
        LANGUAGES.add("af");
        LANGUAGES.add("bn");
        LANGUAGES.add("sw");
        LANGUAGES.add("io");
        LANGUAGES.add("ne");
        LANGUAGES.add("gu");
        LANGUAGES.add("zh-yue");
        LANGUAGES.add("nds");
        LANGUAGES.add("ur");
        LANGUAGES.add("ba");
        LANGUAGES.add("scn");
        LANGUAGES.add("ku");
        LANGUAGES.add("ast");
        LANGUAGES.add("qu");
        LANGUAGES.add("su");
        LANGUAGES.add("diq");
        LANGUAGES.add("tt");
        LANGUAGES.add("ga");
        LANGUAGES.add("ky");
        LANGUAGES.add("cv");
        LANGUAGES.add("ia");
        LANGUAGES.add("nap");
        LANGUAGES.add("bat-smg");
        LANGUAGES.add("map-bms");
        LANGUAGES.add("als");
        LANGUAGES.add("wa");
        LANGUAGES.add("kn");
        LANGUAGES.add("am");
        LANGUAGES.add("gd");
        LANGUAGES.add("ckb");
        LANGUAGES.add("sco");
        LANGUAGES.add("bug");
        LANGUAGES.add("tg");
        LANGUAGES.add("mzn");
        LANGUAGES.add("zh-min-nan");
        LANGUAGES.add("yi");
        LANGUAGES.add("vec");
        LANGUAGES.add("arz");
        LANGUAGES.add("hif");
        LANGUAGES.add("roa-tara");
        LANGUAGES.add("nah");
        LANGUAGES.add("os");
        LANGUAGES.add("sah");
        LANGUAGES.add("mn");
        LANGUAGES.add("sa");
        LANGUAGES.add("pam");
        LANGUAGES.add("hsb");
        LANGUAGES.add("li");
        LANGUAGES.add("mi");
        LANGUAGES.add("si");
        LANGUAGES.add("se");
        LANGUAGES.add("co");
        LANGUAGES.add("gan");
        LANGUAGES.add("glk");
        LANGUAGES.add("bar");
        LANGUAGES.add("fo");
        LANGUAGES.add("ilo");
        LANGUAGES.add("bo");
        LANGUAGES.add("bcl");
        LANGUAGES.add("mrj");
        LANGUAGES.add("fiu-vro");
        LANGUAGES.add("nds-nl");
        LANGUAGES.add("ps");
        LANGUAGES.add("tk");
        LANGUAGES.add("vls");
        LANGUAGES.add("gv");
        LANGUAGES.add("rue");
        LANGUAGES.add("pa");
        LANGUAGES.add("dv");
        LANGUAGES.add("xmf");
        LANGUAGES.add("pag");
        LANGUAGES.add("nrm");
        LANGUAGES.add("kv");
        LANGUAGES.add("zea");
        LANGUAGES.add("koi");
        LANGUAGES.add("km");
        LANGUAGES.add("rm");
        LANGUAGES.add("csb");
        LANGUAGES.add("lad");
        LANGUAGES.add("udm");
        LANGUAGES.add("or");
        LANGUAGES.add("mhr");
        LANGUAGES.add("mt");
        LANGUAGES.add("fur");
        LANGUAGES.add("lij");
        LANGUAGES.add("wuu");
        LANGUAGES.add("ug");
        LANGUAGES.add("pi");
        LANGUAGES.add("sc");
        LANGUAGES.add("zh-classical");
        LANGUAGES.add("frr");
        LANGUAGES.add("bh");
        LANGUAGES.add("nov");
        LANGUAGES.add("ksh");
        LANGUAGES.add("ang");
        LANGUAGES.add("so");
        LANGUAGES.add("stq");
        LANGUAGES.add("kw");
        LANGUAGES.add("nv");
        LANGUAGES.add("hak");
        LANGUAGES.add("ay");
        LANGUAGES.add("frp");
        LANGUAGES.add("vep");
        LANGUAGES.add("ext");
        LANGUAGES.add("pcd");
        LANGUAGES.add("szl");
        LANGUAGES.add("gag");
        LANGUAGES.add("gn");
        LANGUAGES.add("ie");
        LANGUAGES.add("ln");
        LANGUAGES.add("haw");
        LANGUAGES.add("xal");
        LANGUAGES.add("eml");
        LANGUAGES.add("pfl");
        LANGUAGES.add("pdc");
        LANGUAGES.add("rw");
        LANGUAGES.add("krc");
        LANGUAGES.add("crh");
        LANGUAGES.add("ace");
        LANGUAGES.add("to");
        LANGUAGES.add("as");
        LANGUAGES.add("ce");
        LANGUAGES.add("kl");
        LANGUAGES.add("arc");
        LANGUAGES.add("dsb");
        LANGUAGES.add("myv");
        LANGUAGES.add("bjn");
        LANGUAGES.add("pap");
        LANGUAGES.add("sn");
        LANGUAGES.add("tpi");
        LANGUAGES.add("lbe");
        LANGUAGES.add("mdf");
        LANGUAGES.add("wo");
        LANGUAGES.add("kab");
        LANGUAGES.add("jbo");
        LANGUAGES.add("av");
        LANGUAGES.add("lez");
        LANGUAGES.add("srn");
        LANGUAGES.add("cbk-zam");
        LANGUAGES.add("ty");
        LANGUAGES.add("bxr");
        LANGUAGES.add("lo");
        LANGUAGES.add("kbd");
        LANGUAGES.add("ab");
        LANGUAGES.add("tet");
        LANGUAGES.add("mwl");
        LANGUAGES.add("ltg");
        LANGUAGES.add("na");
        LANGUAGES.add("ig");
        LANGUAGES.add("kg");
        LANGUAGES.add("nso");
        LANGUAGES.add("za");
        LANGUAGES.add("kaa");
        LANGUAGES.add("zu");
        LANGUAGES.add("rmy");
        LANGUAGES.add("chy");
        LANGUAGES.add("cu");
        LANGUAGES.add("tn");
        LANGUAGES.add("chr");
        LANGUAGES.add("got");
        LANGUAGES.add("cdo");
        LANGUAGES.add("sm");
        LANGUAGES.add("bi");
        LANGUAGES.add("mo");
        LANGUAGES.add("bm");
        LANGUAGES.add("iu");
        LANGUAGES.add("pih");
        LANGUAGES.add("ss");
        LANGUAGES.add("sd");
        LANGUAGES.add("pnt");
        LANGUAGES.add("ee");
        LANGUAGES.add("om");
        LANGUAGES.add("ha");
        LANGUAGES.add("ki");
        LANGUAGES.add("ti");
        LANGUAGES.add("ts");
        LANGUAGES.add("ks");
        LANGUAGES.add("sg");
        LANGUAGES.add("ve");
        LANGUAGES.add("rn");
        LANGUAGES.add("cr");
        LANGUAGES.add("ak");
        LANGUAGES.add("lg");
        LANGUAGES.add("tum");
        LANGUAGES.add("dz");
        LANGUAGES.add("ny");
        LANGUAGES.add("ik");
        LANGUAGES.add("ff");
        LANGUAGES.add("ch");
        LANGUAGES.add("st");
        LANGUAGES.add("fj");
        LANGUAGES.add("tw");
        LANGUAGES.add("xh");
        LANGUAGES.add("ng");
        LANGUAGES.add("ii");
        LANGUAGES.add("cho");
        LANGUAGES.add("mh");
        LANGUAGES.add("aa");
        LANGUAGES.add("kj");
        LANGUAGES.add("ho");
        LANGUAGES.add("mus");
        LANGUAGES.add("kr");
        LANGUAGES.add("hz");
    }


    private String hostname; // Provided hostname

    private String language = null;
    private String siteVersion = "X";
    private String projectDomain;

    // TODO: work out the actual space of project names and assign them canonical IDs.
    // private String project;          // Canonical ID for this project
    // private String projectName;      // Human-readable name of this project


    public ProjectInfo(String hostname) {
        this.hostname = hostname;

        String[] domainParts = hostname.split("\\.");
        List<String> projectParts = Lists.newArrayListWithExpectedSize(domainParts.length);

        // Ignore SLD+TLD, as language/version are always before (andalso country TLDs look like languages)
        for (int i = 0; i < domainParts.length - 2; i++) {
            String part = domainParts[i];
            if (part.equalsIgnoreCase("www")) {
                continue;
            } else if (language == null && LANGUAGES.contains(part)) {
                language = part;
            } else if (SITE_VERSIONS.containsKey(part)) {
                siteVersion = SITE_VERSIONS.get(part);
            } else {
                projectParts.add(part);
            }
        }

        // Add SLD+TLD which we skipped above
        projectParts.add(domainParts[domainParts.length - 2]);
        projectParts.add(domainParts[domainParts.length - 1]);
        projectDomain = Joiner.on('.').skipNulls().join(projectParts);
    }

    public String getHostname() {
        return hostname;
    }

    public String getLanguage() {
        return language;
    }

    public String getSiteVersion() {
        return siteVersion;
    }

    public String getProjectDomain() {
        return projectDomain;
    }
}
