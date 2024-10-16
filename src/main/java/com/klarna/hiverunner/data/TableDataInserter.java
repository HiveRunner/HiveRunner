/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) ${license.git.copyrightYears} The HiveRunner Contributors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.klarna.hiverunner.data;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

import java.util.Iterator;
import java.util.Map;

class TableDataInserter {

    private final String databaseName;
    private final String tableName;
    private final Map<String, String> config;

    TableDataInserter(String databaseName, String tableName, HiveConf conf) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        config = Maps.fromProperties(conf.getAllProperties());
    }

    void insert(Multimap<Map<String, String>, HCatRecord> data) {
        Iterator<Map<String, String>> iterator = data.keySet().iterator();
        while (iterator.hasNext()) {
            Map<String, String> partitionSpec = iterator.next();
            insert(partitionSpec, data.get(partitionSpec));
        }
    }

    private void insert(Map<String, String> partitionSpec, Iterable<HCatRecord> rows) {
        WriteEntity entity = new WriteEntity.Builder()
                .withDatabase(databaseName)
                .withTable(tableName)
                .withPartition(partitionSpec)
                .build();

        try {
            HCatWriter master = DataTransferFactory.getHCatWriter(entity, config);
            WriterContext context = master.prepareWrite();
            HCatWriter writer = DataTransferFactory.getHCatWriter(context);
            writer.write(rows.iterator());
            master.commit(context);
        } catch (HCatException e) {
            throw new RuntimeException("An error occurred while inserting data to " + databaseName + "." + tableName, e);
        }
    }

}
