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


import java.net.URL;
import java.util.Arrays;

/**
 *
 */
public class PageviewCanonical {
    private StringBuilder sb;


    /**
     *
     * @param url
     * @return
     */
    public String canonicalizeDesktopPageview(final URL url) {
        sb = new StringBuilder();
        String[] hostname = url.getHost().split("\\.");
        sb.append(hostname[0]);
        sb.append(".");
        sb.append(hostname[1]);
        sb.append(" ");
        sb.append(url.getPath().replace("/wiki/", ""));
        return sb.toString();
    }

    /**
     *
     * @param url
     * @return
     */
    public String canonicalizeMobilePageview(final URL url) {
        sb = new StringBuilder();
        String[] hostname = url.getHost().split("\\.");
        sb.append(hostname[0]);
        sb.append(".");
        sb.append(hostname[1]);
        sb.append(".");
        sb.append(hostname[2]);
        sb.append(" ");
        sb.append(url.getPath().replace("/wiki/", ""));
        return sb.toString();
    }

    /**
     *
     * @param url
     * @return
     */
    public String canonicalizeApiRequest(final URL url) {
        //TODO not yet implemented
        return url.toString();
    }

    /**
     *
     * @param url
     * @return
     */
    public String canonicalizeBlogPageview(final URL url) {
        //TODO not yet implemented
        return url.toString();
    }

    /**
     *
     * @param url
     * @return
     */
    public String canonicalizeSearchQuery(final URL url) {
        return url.toString();
    }

    /**
     * This function canonicalizes an imageview as follows:
     * Given thumbail view https://upload.wikimedia.org/wikipedia/commons/thumb/1/19/Acueducto_de_Segovia_01.jpg/600px-Acueducto_de_Segovia_01.jpg
     * that becomes https://upload.wikimedia.org/wikipedia/commons/1/19/Acueducto_de_Segovia_01.jpg
     * @param url
     * @return
     */
    public String canonicalizeImagePageview(final URL url) {
        int positionRightSlash = url.getPath().lastIndexOf("/");
        String path = url.getPath().replace("thumb", "").substring(0, positionRightSlash);

        sb = new StringBuilder();
        sb.append(url.getHost());
        sb.append("/");
        sb.append(path);
        return sb.toString();
    }
}
