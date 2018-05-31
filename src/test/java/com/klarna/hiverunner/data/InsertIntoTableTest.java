/**
 * Copyright (C) 2013-2018 Klarna AB
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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Multimap;

@RunWith(MockitoJUnitRunner.class)
public class InsertIntoTableTest {

  @Mock
  private TableDataBuilder builder;
  @Mock
  private TableDataInserter inserter;

  private InsertIntoTable insert;

  @Before
  public void before() {
    insert = new InsertIntoTable(builder, inserter);
  }

  @Test
  public void withColumns() {
    insert.withColumns(any(String[].class));

    verify(builder).withColumns(any(String[].class));
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
    insert.addRow(any(Object[].class));

    verify(builder).addRow(any(Object[].class));
  }

  @Test
  public void setRow() {
    insert.setRow(any(Object[].class));

    verify(builder).setRow(any(Object[].class));
  }

  @Test
  public void addRows() {
    insert.addRowsFromTsv(any(File.class));

    verify(builder).addRowsFromTsv(any(File.class));
  }

  @Test
  public void addRowsWithFileParser() {
    insert.addRowsFrom(any(File.class), any(FileParser.class));

    verify(builder).addRowsFrom(any(File.class), any(FileParser.class));
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
    insert.commit();

    verify(builder).build();
    verify(inserter).insert(any(Multimap.class));

  }
}
