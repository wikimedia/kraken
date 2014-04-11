/**
 *Copyright (C) 2012-2013  Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

/**
 * The JsonToClassConverter is a generic class that can load any file that contains a list
 * of JSON objects into a {@link HashMap} with the key specified by the caller and the value
 * an instance of the JSON object
 */
public class JsonToClassConverter {

    /**
     * @param className refers to the name of the class that maps to the JSON file. Make sure that
     * all properties in the JSON file are defined in the Java class, else it will throw an error
     * @param file contains the name of the JSON file to be loaded. The default place to put this
     * file is in the src/main/resource folder
     * @param key name of the field from the JSON object that should be used as key to store the
     * JSON object in the HashMap. Suppose the field containing the key is called 'foo' then the
     * java Class should have a getter called getFoo.
     * @return
     * @throws JsonMappingException
     * @throws JsonParseException
     */
    public final HashMap<String, Schema> construct(final String className, final String file, final String key)
            throws JsonMappingException, JsonParseException {
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
            schemas = mapper.readValue(jParser, type);
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
            for (Schema schemaInstance: schemas) {
                try {
                    Method getKey = schemaInstance.getClass().getMethod(key);
                    map.put(getKey.invoke(schemaInstance).toString(), schemaInstance);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                } catch (NoSuchMethodException e) {
                    System.err.println("Specified key is not a valid Getter for " + className);
                }
            }
        }
        return map;
    }
}
