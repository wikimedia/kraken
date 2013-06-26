/**
 *Copyright (C) 2012-2013  Wikimedia Foundation
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

 */
package org.wikimedia.analytics.kraken.pageview;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.List;
import java.util.ListIterator;


/**
 * This class contains detailed business logic to simplify a the url of a valid pageview into the canonical title
 * of the page.
 */
public class PageviewCanonical {

    private Charset charset = Charset.defaultCharset();

    private URL url;

    private PageviewType pageviewType;

    private String articleTitle;

    /**
     *
     * @param url
     */
    public PageviewCanonical(final URL url) {
        this.url = url;
        pageviewType = PageviewType.determinePageviewType(url);
    }

    /**
     *
     */
    public final void canonicalize(final String mode) {
        switch (pageviewType) {

            case API:
                extractMediawikiApiTitle(mode);

            case REGULAR:
                extractMediawikiRegularTitle(mode);

            case BLOG:
                canonicalizeBlogPageview();

            case IMAGE:
                canonicalizeImagePageview();

            case BANNER:
                break;

            case SEARCH:
                canonicalizeSearchQuery();

            case OTHER:
                break;

            case NONE:
                break;

            default:
                break;
        }
    }

    /**
     *
     * @return
     */
    private void extractMediawikiRegularTitle(final String mode) {
        if (url != null && url.getPath() != null) {
            if (url.getPath().contains("/wiki/")) {
                this.articleTitle = url.getPath().replaceAll("/wiki/", "");
            } else if (url.getQuery() != null && url.getPath().contains("index.php")) {
                String[] keys = {"title"};
                if (mode.equals("default")) {
                    this.articleTitle = searchQueryAction(keys);
                } else {
                 this.articleTitle = url.getPath();
                }
            }
        }
    }

    /**
     *
     * @param url
     * @return
     * @throws MalformedURLException
     */
    private URL fixApacheHttpComponentBug(final URL url) throws MalformedURLException {
        return new URL(url.toString().replace(";", "&"));
    }

    /**
     * Enter one or more keys to search for, this list of keys is
     * interpreted as key1 or key2; this function is not intended
     * to retrieve the values of multiple keys. In that case,
     * call this function multiple times.
     * @param keys
     * @return
     */
    private String searchQueryAction(final String[] keys) {
        try {
            URL pURL = fixApacheHttpComponentBug(url);

            List<NameValuePair> qparams = URLEncodedUtils.parse(pURL.toURI(), "utf-8");
            ListIterator<NameValuePair> it = qparams.listIterator();
            while (it.hasNext()) {
                NameValuePair nvp = it.next();
                for (String key : keys) {
                    if (nvp.getName().equals(key)) {
                        return nvp.getValue();
                    }
                }
            }
        } catch (URISyntaxException e) {
            return "key.not.found";
        } catch (MalformedURLException e) {
            return "malformed.url";
        }
        return "key.not.found";
    }

    /**
     * @return
     */
    private void extractMediawikiApiTitle(final String mode) {
        if (url != null && url.getQuery() != null) {
                if (mode.equals("default")) {
                    String[] keys = {"page", "titles"};
                    String tempTitle = searchQueryAction(keys);
                    this.articleTitle = convertApiTitleToRegularArticleTitle(tempTitle);
                } else {
                    this.articleTitle = url.getPath();
                }
            }
        }


    /**
     *
     * @param apiTitle
     * @return
     */
    private String convertApiTitleToRegularArticleTitle(final String apiTitle) {
        return apiTitle.replaceAll(" ", "_");
    }

    /**
     * This function is specifically written for PageviewType.IMAGE pageviews.
     * @param url
     * @return
     */
    private String parsePath(final URL url) {
        // http://upload.wikimedia.org/wikipedia/commons/thumb/8/87/Nakhalfarms.jpg/220px-Nakhalfarms.jpg
        String pathWithoutPrefix = url.getPath().replaceAll("/wikipedia/[a-z]*/thumb/[a-z0-9]{1}/[a-z0-9]{2}/", "");
        int positionRightSlash = pathWithoutPrefix.lastIndexOf("/");
        String pathWithoutThumb;
        if (positionRightSlash > 0) {
            pathWithoutThumb = pathWithoutPrefix.substring(0, positionRightSlash);
        } else {
            pathWithoutThumb = pathWithoutPrefix;
        }

        String path;
        if (!pathWithoutThumb.endsWith(".jpg")
                || !pathWithoutThumb.endsWith(".png")
                || !pathWithoutThumb.endsWith(".svg")) {
            positionRightSlash = pathWithoutThumb.lastIndexOf("/");
            if (positionRightSlash > 0) {
                path = pathWithoutThumb.substring(0, positionRightSlash);
            } else {
                path =  pathWithoutThumb;
            }
        }  else {
            path =  pathWithoutThumb;
        }
        return path;
    }

    /**
     *
     * @return
     */
    private void canonicalizeBlogPageview() {
        //TODO not yet implemented
    }

    /**
     *
     * @return
     */
    private void canonicalizeSearchQuery() {
        //TODO not yet implemented
    }

    /**
     * This function canonicalizes an imageview as follows:
     * Given thumbail view https://upload.wikimedia.org/wikipedia/commons/thumb/1/19/Acueducto_de_Segovia_01.jpg/600px-Acueducto_de_Segovia_01.jpg
     * that becomes upload Acueducto_de_Segovia_01.jpg
     * @return
     */
    public final void canonicalizeImagePageview() {
        //TODO implementation not yet finished.
        String path = parsePath(url);
    }

    /**
     *
     * @return
     */
    public final String getArticleTitle() {
        return articleTitle;
    }
}
