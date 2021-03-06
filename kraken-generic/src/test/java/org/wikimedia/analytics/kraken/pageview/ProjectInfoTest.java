package org.wikimedia.analytics.kraken.pageview;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import static com.google.common.collect.ImmutableList.of;
import static org.junit.Assert.assertEquals;

public class ProjectInfoTest {

    private static List<ImmutableList<String>> TESTS = Lists.newArrayList(
            // testHostname                       projectDomain                      language        siteVersion
            of("mediawiki.org",                   "mediawiki.org",                   "",              "X"),
            of("en.wikipedia.org",                "wikipedia.org",                   "en",            "X"),
            of("simple.wikipedia.org",            "wikipedia.org",                   "simple",        "X"),
            of("be-x-old.wikipedia.org",          "wikipedia.org",                   "be-x-old",      "X"),
            of("zh-classical.wikipedia.org",      "wikipedia.org",                   "zh-classical",  "X"),
            of("zh-min-nan.wiktionary.org",       "wiktionary.org",                  "zh-min-nan",    "X"),
            of("ja.m.wikipedia.org",              "wikipedia.org",                   "ja",            "M"),
            of("ckb.m.wikipedia.org",             "wikipedia.org",                   "ckb",           "M"),
            of("zh-yue.m.wikipedia.org",          "wikipedia.org",                   "zh-yue",        "M"),
            of("simple.m.wikipedia.org",          "wikipedia.org",                   "simple",        "M"),
            of("mg.zero.wikipedia.org",           "wikipedia.org",                   "mg",            "Z"),
            of("en.mobile.wikipedia.org",         "wikipedia.org",                   "en",            "M"),
            of("cs.m.wikibooks.org",              "wikibooks.org",                   "cs",            "M"),
            of("species.m.wikimedia.org",         "species.wikimedia.org",           "",              "M"),
            of("test.wikipedia.org",              "test.wikipedia.org",              "",              "X"),
            of("beta.wikiversity.org",            "beta.wikiversity.org",            "",              "X"),
            of("donate.wikimedia.org",            "donate.wikimedia.org",            "",              "X"),
            of("quote.wikipedia.org",             "quote.wikipedia.org",             "",              "X"),
            of("www.mediawiki.org",               "mediawiki.org",                   "",              "X"),
            of("www.wikimedia.org",               "wikimedia.org",                   "",              "X"),
            of("www.wikidata.org",                "wikidata.org",                    "",              "X"),
            of("www.wikivoyage.org",              "wikivoyage.org",                  "",              "X"),
            of("www.wikimedia.com",               "wikimedia.com",                   "",              "X"),
            of("en.labs.wikimedia.org",           "labs.wikimedia.org",              "en",            "X"),
            of("flaggedrevs.labs.wikimedia.org",  "flaggedrevs.labs.wikimedia.org",  "",              "X"),
            of("127.0.0.1",                       "127.0.0.1",                       "",              "X")
        );

    static private String get(ImmutableList<String> test, int idx) {
        return test.get(idx).equalsIgnoreCase("null") ? null : test.get(idx);
    }
    
    @Test
    public void testDomains() throws Exception {
        for (ImmutableList<String> test : TESTS) {
            String host = test.get(0);
            ProjectInfo info = new ProjectInfo(host);

            assertEquals("Incorrect hostname for "+ host,        host,        info.getHostname());
            assertEquals("Incorrect projectDomain for "+ host,   get(test,1), info.getProjectDomain());
            assertEquals("Incorrect language for " + host,       get(test,2), info.getLanguage());
            assertEquals("Incorrect siteVersion for "+ host,     get(test,3), info.getSiteVersion());
        }
    }

    @Test
    public void testFixVarnishNCSALoggingBug() throws MalformedURLException {
        URL url = new URL("http://fr.m.wikipedia.orghttp://fr.m.wikipedia.org/wiki/Wikip%C3%A9dia:Accueil_principal");
        ProjectInfo info = new ProjectInfo(url.getHost());
        assertEquals("wikipedia.org", info.getProjectDomain());
        assertEquals("M", info.getSiteVersion());
        assertEquals("fr", info.getLanguage());
    }

    @Test
    public void testDomainWithoutLanguageCode() throws MalformedURLException {
        URL url = new URL("http://m.wikipedia.org/wiki/Foo");
        ProjectInfo info = new ProjectInfo(url.getHost());
        assertEquals("", info.getLanguage());

    }
}
