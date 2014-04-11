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

/**
 * This class provides the mapping to the country-codes.json file in src/main/resources.
 * The guaranteed unique field in this class is either the 'a2' or 'a3' fields.
 */
public class Country extends Schema {
    public String id;
    public String name;
    public Integer num;
    public String ioc;
    public String a2;
    public String a3;
    public String itu;
    public String fifa;
    public String continentCode;
    public String continentName;

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public String getIoc() {
        return ioc;
    }

    public void setIoc(String ioc) {
        this.ioc = ioc;
    }

    public String getA2() {
        return a2;
    }

    public void setA2(String a2) {
        this.a2 = a2;
    }

    public String getA3() {
        return a3;
    }

    public void setA3(String a3) {
        this.a3 = a3;
    }

    public String getItu() {
        return itu;
    }

    public void setItu(String itu) {
        this.itu = itu;
    }

    public String getFifa() {
        return fifa;
    }

    public void setFifa(String fifa) {
        this.fifa = fifa;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getContinentCode() {
        return continentCode;
    }

    public void setContinentCode(String continentCode) {
        this.continentCode = continentCode;
    }

    public String getContinentName() {
        return continentName;
    }

    public void setContinentName(String continentName) {
        this.continentName = continentName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
