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


/**
 *
 */
public class PageviewCanonical {
    private StringBuilder sb;



    private String getProject(final URL url, final PageviewType pageviewType){
        sb = new StringBuilder();
        String[] hostname = url.getHost().split("\\.");
        if (pageviewType == PageviewType.MOBILE || pageviewType == PageviewType.MOBILE_API) {
            sb.append(hostname[0]);
            sb.append(".");
            sb.append(hostname[1]);
            sb.append(".");
            sb.append(hostname[2]);
        }  else if (pageviewType == PageviewType.IMAGE) {
            sb.append(hostname[0]);
        } else {
            sb.append(hostname[0]);
            sb.append(".");
            sb.append(hostname[1]);
        }
        return sb.toString();

    }
    /**
     *
     * @param url
     * @parm pageviewType
     * @return
     */
    public String canonicalizeDesktopPageview(final URL url, final PageviewType pageviewType) {
        String project = getProject(url, pageviewType);
        sb = new StringBuilder();
        sb.append(project);
        sb.append(" ");
        sb.append(url.getPath().replace("/wiki/", ""));
        return sb.toString();
    }

    /**
     *
     * @param url
     * @parm pageviewType
     * @return
     */
    public String canonicalizeMobilePageview(final URL url, final PageviewType pageviewType) {
        String project = getProject(url, pageviewType);
        sb = new StringBuilder();
        sb.append(project);
        sb.append(" ");
        sb.append(url.getPath().replace("/wiki/", ""));
        return sb.toString();
    }

    /**
     *
     * @param url
     * @return
     */
    public String canonicalizeApiRequest(final URL url, final PageviewType pageviewType) {
        //TODO not yet implemented
        return url.toString();
    }

    /**
     *
     * @param url
     * @parm pageviewType
     * @return
     */
    public String canonicalizeBlogPageview(final URL url, final PageviewType pageviewType) {
        //TODO not yet implemented
        return url.toString();
    }

    /**
     *
     * @param url
     * @parm pageviewType
     * @return
     */
    public String canonicalizeSearchQuery(final URL url, final PageviewType pageviewType) {
        return url.toString();
    }

    /**
     * This function canonicalizes an imageview as follows:
     * Given thumbail view https://upload.wikimedia.org/wikipedia/commons/thumb/1/19/Acueducto_de_Segovia_01.jpg/600px-Acueducto_de_Segovia_01.jpg
     * that becomes https://upload.wikimedia.org/wikipedia/commons/1/19/Acueducto_de_Segovia_01.jpg
     * @param url
     * @parm pageviewType
     * @return
     */
    public String canonicalizeImagePageview(final URL url, final PageviewType pageviewType) {
        int positionRightSlash = url.getPath().lastIndexOf("/");
        String path = url.getPath().replace("thumb", "").substring(0, positionRightSlash);
        String project = getProject(url, pageviewType);

        sb = new StringBuilder();
        sb.append(project);
        sb.append(" ");
        sb.append(path);
        return sb.toString();
    }
}
