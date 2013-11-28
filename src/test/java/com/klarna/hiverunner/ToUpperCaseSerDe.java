/*
 * Copyright 2013 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.klarna.hiverunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ToUpperCaseSerDe extends AbstractSerDe {

    private List<String> columns;

    @Override
    public void initialize(Configuration configuration, Properties properties) throws SerDeException {
        columns = Arrays.asList(((String) properties.get(Constants.LIST_COLUMNS)).split(","));
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        throw new SerDeException("Not implemented in test fixture");
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        String[] values = writable.toString().toUpperCase().split(",");
        return Arrays.asList(values);
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        // Constructing the row ObjectInspector:
        // The row consists of some string columns, each column will be a java
        // String object.
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        // Standard Struct uses ArrayList to store the row.
        return ObjectInspectorFactory.getStandardStructObjectInspector(columns, columnOIs);

    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }
}

