/*
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021 The HiveRunner Contributors
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
package com.klarna.hiverunner.data;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Map;

import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.Multimap;

@ExtendWith(MockitoExtension.class)
public class InsertIntoTableTest {

  @Mock
  private TableDataBuilder builder;
  @Mock
  private TableDataInserter inserter;

  private InsertIntoTable insert;

  @BeforeEach
  public void before() {
    insert = new InsertIntoTable(builder, inserter);
  }

  @Test
  public void withColumns() {
    String[] columns = new String[] { "columnA", "columnB" };
    insert.withColumns(columns);

    verify(builder).withColumns(columns);
  }

  @Test
  public void withAllColumns() {
    insert.withAllColumns();

    verify(builder).withAllColumns();
  }

  @Test
  public void newRow() {
    insert.newRow();

    verify(builder).newRow();
  }

  @Test
  public void addRow() {
    Object[] row = new Object[] { "columnA" };
    insert.addRow(row);

    verify(builder).addRow(row);
  }

  @Test
  public void setRow() {
    Object[] row = new Object[] { "columnA" };
    insert.setRow(row);

    verify(builder).setRow(row);
  }

  @Test
  public void addRows() {
    File file = new File("foo");
    insert.addRowsFromTsv(file);

    verify(builder).addRowsFromTsv(file);
  }

  @Test
  public void addRowsWithFileParser() {
    File file = new File("foo");
    FileParser parser = new TsvFileParser();
    insert.addRowsFrom(file, parser);

    verify(builder).addRowsFrom(file, parser);
  }

  @Test
  public void copyRow() {
    insert.copyRow();

    verify(builder).copyRow();
  }

  @Test
  public void set() {
    insert.set("a", "b");

    verify(builder).set("a", "b");
  }

  @Test
  public void commit() {
    Multimap<Map<String, String>, HCatRecord> map = mock(Multimap.class);
    when(builder.build()).thenReturn(map);
    insert.commit();

    verify(inserter).insert(map);

  }
}
