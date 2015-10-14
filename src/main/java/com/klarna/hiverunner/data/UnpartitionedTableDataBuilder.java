package com.klarna.hiverunner.data;

import static com.google.common.base.Preconditions.checkState;

import java.util.List;

import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.data.HCatRecord;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

class UnpartitionedTableDataBuilder extends TableDataBuilder<List<HCatRecord>> {

  final Builder<HCatRecord> recordsBuilder = ImmutableList.builder();

  UnpartitionedTableDataBuilder(HCatTable table, TableDataInserter inserter) {
    super(table, inserter);
  }

  @Override
  protected void flushRow() {
    if (record != null) {
      recordsBuilder.add(record);
    }
  }

  @Override
  protected List<HCatRecord> build() {
    flushRow();
    List<HCatRecord> records = recordsBuilder.build();
    checkState(records.size() > 0, "No data.");
    return records;
  }

  @Override
  public void commit() {
    inserter.insert(null, build());
  }

}
