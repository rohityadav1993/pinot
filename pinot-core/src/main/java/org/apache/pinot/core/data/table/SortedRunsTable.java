/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.data.table;

import java.util.Iterator;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;


/// A read-only [Table] over records that are already unique per key and already final for the block they travel in.
///
/// The sorted streaming GROUP BY combine emits finished groups in key order; this table only lets those rows ride in a
/// [org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock], whose rows and data table are backed by a
/// [Table]. It does no hashing, trimming or merging. Not thread-safe; a block's table is built, handed downstream and
/// never mutated again.
public class SortedRunsTable implements Table {
  private final DataSchema _dataSchema;
  private final List<Record> _records;

  public SortedRunsTable(DataSchema dataSchema, List<Record> records) {
    _dataSchema = dataSchema;
    _records = records;
  }

  @Override
  public boolean upsert(Record record) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean upsert(Key key, Record record) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean merge(Table table) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return _records.size();
  }

  @Override
  public Iterator<Record> iterator() {
    return _records.iterator();
  }

  @Override
  public void finish(boolean sort, boolean storeFinalResult) {
    // Records are final when added.
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }
}
