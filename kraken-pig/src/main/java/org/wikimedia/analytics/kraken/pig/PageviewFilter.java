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
package org.wikimedia.analytics.kraken.pig;


import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PageviewFilter {

    public final Pattern responsePattern = Pattern.compile("$.*//\\d{3}");
    public final Pattern mimeTypePattern = Pattern.compile("");

    public boolean isValidResponse(String response) {
        Matcher match = responsePattern.matcher(response);
        if (match.matches()) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isValidMimeType(String mimeType) {
        Matcher match = mimeTypePattern.matcher(mimeType);
        if (match.matches()) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isInternalWMFTraffic(String ip) {
        //TODO: not yet implemented
        return false;
    }


    /**
     * Ignore the following paths:
     * http://testblog.wikimedia.org
     * http://blog.wikimedia.org/wp-login.php
     * http://blog.wikimedia.org/wp-admin/
     * http://blog.wikimedia.org/?s=  (i.e. searches)
    */
    public boolean isValidBlogURL(URL url) {
        if (url != null && (
                url.getPath().startsWith("wp-") ||
                url.getPath().startsWith("?s=") ||
                url.getHost().startsWith("test"))) {
            return false;
        }
        return true;
    }


}
