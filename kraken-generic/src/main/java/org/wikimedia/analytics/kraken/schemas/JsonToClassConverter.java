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

package org.wikimedia.analytics.kraken.schemas;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;


public class JsonToClassConverter {

    public HashMap<String, Schema> construct(String className, String file, String key)
            throws JsonMappingException, JsonParseException, RuntimeException {
        JsonFactory jfactory = new JsonFactory();
        HashMap<String, Schema> map = new HashMap<String, Schema>();
        List<Schema> schemas = null;
        InputStream input;
        JavaType type;
        ObjectMapper mapper = new ObjectMapper();

        try {
            Schema schema = (Schema) Schema.class
                        .getClassLoader()
                        .loadClass(className)
                        .newInstance();

        input = schema.getClass().getClassLoader().getResourceAsStream(file);

        type = mapper.getTypeFactory().
                constructCollectionType(List.class, schema.getClass());
        }  catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        try {
            JsonParser jParser = jfactory.createJsonParser(input);
            schemas = mapper.readValue(jParser,type);
        } catch (IOException e) {
            System.err.println("Specified file could not be found.");
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException e){
                System.err.println("Could not close filestream");
            }
        }
        if (schemas != null){
            for(Schema schemaInstance: schemas) {
                try {
                    Method getKey = schemaInstance.getClass().getMethod(key);
                    map.put(getKey.invoke(schemaInstance).toString(), schemaInstance);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                } catch (NoSuchMethodException e){
                    System.err.println("Specified key is not a valid Getter for " + className);
                }
            }
        }
        return map;
    }
}
