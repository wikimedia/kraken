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


import java.util.ArrayList;

public class Field {
    private enum Type {
        STRING, INTEGER, BOOLEAN, ARRAYLIST
    }

    private final Type type;
    private final Object value;

    private Field(Object value, Type type) {
        this.value = value;
        this.type = type;
    }

    public static Field fromString(String s) {
        return new Field(s, Type.STRING);
    }

    public static Field fromInteger(Integer i) {
        return new Field(i, Type.INTEGER);
    }

    public static Field fromBoolean(Boolean b) {
        return new Field(b, Type.BOOLEAN);
    }

    public static Field fromArrayListString(ArrayList<String> a) {
        return new Field (a, Type.ARRAYLIST);
    }

    public Type getType() {
        return this.type;
    }

    public String getStringValue() {
        return (String) value;
    }

    public Integer getIntegerValue() {
        return (Integer) value;
    }

    public Boolean getBooleanValue() {
        return (Boolean) value;
    }

    public ArrayList<String> getArrayListString() {
        //Not yet implemented
        return new ArrayList<String>();
    }
    // equals, hashCode
}
