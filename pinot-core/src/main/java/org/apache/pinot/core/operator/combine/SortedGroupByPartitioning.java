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
package org.apache.pinot.core.operator.combine;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.Schema;


/// Splits the segment operators of a sorted streaming GROUP BY into independent merge groups by the partition of the
/// group-by column, using segment metadata only (no segment is acquired).
///
/// **When it splits.** Only when every segment reports, for the group-by column, the same partition function (by
/// [PartitionFunction#equals]) and exactly one partition id. A partition function maps one key to one partition, so
/// two segments of different partitions can never hold the same group key, and the partitions can be merged
/// independently and in parallel. A segment without partition metadata, with several partitions, or under a different
/// function (or number of partitions) makes the whole operator list one group, which is always correct.
///
/// **Result.** Groups are ordered by ascending partition id and keep the operators' relative order inside a group.
///
/// Thread-safety: stateless.
final class SortedGroupByPartitioning {
  private SortedGroupByPartitioning() {
  }

  /// The outcome of [#split]. `_partitionIds` has one entry per group when the split is by partition, and is `null`
  /// for the single unpartitioned group, in which case `_reason` says why it was not split.
  static final class Split {
    final List<List<Operator>> _groups;
    @Nullable
    final List<Integer> _partitionIds;
    @Nullable
    final String _reason;

    Split(List<List<Operator>> groups, @Nullable List<Integer> partitionIds, @Nullable String reason) {
      _groups = groups;
      _partitionIds = partitionIds;
      _reason = reason;
    }

    boolean isPartitioned() {
      return _partitionIds != null;
    }
  }

  static Split split(List<Operator> operators, String column, Schema schema) {
    if (operators.size() < 2) {
      return single(operators, "fewer than two segments");
    }
    PartitionFunction function = null;
    TreeMap<Integer, List<Operator>> groups = new TreeMap<>();
    for (Operator operator : operators) {
      IndexSegment segment = operator.getIndexSegment();
      if (segment == null) {
        return single(operators, "a segment is not known");
      }
      DataSourceMetadata metadata = segment.getDataSource(column, schema).getDataSourceMetadata();
      PartitionFunction segmentFunction = metadata.getPartitionFunction();
      Set<Integer> partitions = metadata.getPartitions();
      if (segmentFunction == null || partitions == null) {
        return single(operators, "segment " + segment.getSegmentName() + " has no partition metadata for " + column);
      }
      if (partitions.size() != 1) {
        return single(operators, "segment " + segment.getSegmentName() + " spans " + partitions.size()
            + " partitions of " + column);
      }
      if (function == null) {
        function = segmentFunction;
      } else if (!function.equals(segmentFunction)) {
        return single(operators, "segments of " + column + " use different partition functions");
      }
      groups.computeIfAbsent(partitions.iterator().next(), k -> new ArrayList<>()).add(operator);
    }
    return new Split(new ArrayList<>(groups.values()), new ArrayList<>(groups.keySet()), null);
  }

  private static Split single(List<Operator> operators, String reason) {
    return new Split(List.of(operators), null, reason);
  }
}
