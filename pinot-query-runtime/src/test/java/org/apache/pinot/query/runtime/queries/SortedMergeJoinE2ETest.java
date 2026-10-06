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
package org.apache.pinot.query.runtime.queries;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxMergeReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.MultiStageStatsTreeBuilder;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/// End-to-end correctness tests of the streaming sorted merge join (`join_strategy='sorted'`) in the multi-stage
/// engine. Every query is executed against two in-process servers and compared with H2, over:
///
/// - table shapes: OFFLINE only, hybrid (OFFLINE + REALTIME with a time boundary), OFFLINE joined with hybrid,
///   hash-partitioned OFFLINE (no hint), and partitioned OFFLINE with `is_colocated_by_join_keys='true'`;
/// - paths: gate on (k-way merge receive), gate off (plain exchange + sort above it), gate on with the leaf sort
///   pushdown enabled through `sortedSelectionMergeMode`, and the default hash join as the baseline.
///
/// Each run also asserts, from the plan and from the runtime stats, that the expected path was taken, so a query
/// cannot pass by silently falling back to the hash join.
///
/// Thread-safety: the tests mutate the shared query environment and must run sequentially (the TestNG default).
public class SortedMergeJoinE2ETest extends QueryRunnerTestBase {
  private static final String SORTED_HINT = "join_strategy='sorted'";
  private static final String PARTITION_HINT =
      "/*+ tableOptions(partition_function='hashcode', partition_key='k_int', partition_size='4') */";
  private static final int NUM_PARTITIONS = 4;
  private static final int HOT_KEY = 7;

  private QueryEnvironment _gateOnEnvironment;
  private QueryEnvironment _gateOffEnvironment;

  /// Table shapes, the three tables are substituted for `{A}`, `{B}`, `{C}`.
  enum Shape {
    OFFLINE("t1", "t2", "t3", false),
    HYBRID("h1", "h2", "h3", false),
    MIXED("t1", "h2", "t3", false),
    PARTITIONED("p1", "p2", "p3", false),
    COLOCATED("p1", "p2", "p3", true);

    final String[] _tables;
    final boolean _colocated;

    Shape(String a, String b, String c, boolean colocated) {
      _tables = new String[]{a, b, c};
      _colocated = colocated;
    }

    String table(int i) {
      return _colocated ? _tables[i] + " " + PARTITION_HINT : _tables[i];
    }
  }

  /// Execution paths.
  enum Path {
    /// Default hash join, the reference for the sorted runs.
    HASH,
    /// Sorted join, gate on.
    MERGE,
    /// Sorted join, gate off (rolling upgrade fallback).
    GATE_OFF,
    /// Sorted join, gate on, `sortedSelectionMergeMode='ON'` so single-type leaves absorb the sender sort.
    MERGE_PUSHDOWN,
    /// Same with AUTO, which the planner treats like ON.
    MERGE_AUTO
  }

  /// A query template. `{H}` is replaced by the join hint comment, `{A}`/`{B}`/`{C}` by tables of the shape.
  static final class Q {
    final String _name;
    final String _sql;
    final int _numJoins;
    /// Query is valid with the colocated hint (INT key join on directly scanned partitioned tables).
    boolean _colocatable;
    /// The result is fully ordered, compare rows in order.
    boolean _ordered;
    /// Inputs of every join are plain scans of the table, so the sender sort can be absorbed into the leaf request.
    boolean _plainInputs;
    final boolean _needsC;

    Q(String name, String sql, int numJoins) {
      _name = name;
      _sql = sql;
      _numJoins = numJoins;
      _needsC = sql.contains("{C}");
    }

    Q colocatable() {
      _colocatable = true;
      return this;
    }

    Q ordered() {
      _ordered = true;
      return this;
    }

    Q plain() {
      _plainInputs = true;
      return this;
    }

    @Override
    public String toString() {
      return _name;
    }
  }

  static final class Case {
    final Q _query;
    final Shape _shape;
    final Path _path;

    Case(Q query, Shape shape, Path path) {
      _query = query;
      _shape = shape;
      _path = path;
    }

    @Override
    public String toString() {
      return _shape + "/" + _path + "/" + _query._name;
    }
  }

  //@formatter:off
  static List<Q> queries() {
    List<Q> qs = new ArrayList<>();
    // single key, every type, INNER and LEFT
    qs.add(new Q("inner_int", "SELECT {H} a.k_int, a.v, b.v FROM {A} a JOIN {B} b ON a.k_int = b.k_int", 1)
        .colocatable().plain());
    qs.add(new Q("left_int", "SELECT {H} a.k_int, a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_int = b.k_int", 1)
        .colocatable().plain());
    qs.add(new Q("inner_long", "SELECT {H} a.k_long, a.v, b.v FROM {A} a JOIN {B} b ON a.k_long = b.k_long", 1)
        .plain());
    qs.add(new Q("left_long", "SELECT {H} a.k_long, a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_long = b.k_long", 1)
        .plain());
    qs.add(new Q("inner_str", "SELECT {H} a.k_str, a.v, b.v FROM {A} a JOIN {B} b ON a.k_str = b.k_str", 1).plain());
    qs.add(new Q("left_str", "SELECT {H} a.k_str, a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_str = b.k_str", 1)
        .plain());
    qs.add(new Q("inner_dbl", "SELECT {H} a.k_dbl, a.v, b.v FROM {A} a JOIN {B} b ON a.k_dbl = b.k_dbl", 1).plain());
    qs.add(new Q("left_dbl", "SELECT {H} a.k_dbl, a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_dbl = b.k_dbl", 1)
        .plain());
    // multi column keys, same and mixed types
    qs.add(new Q("inner_int_str",
        "SELECT {H} a.k_int, a.k_str, a.v, b.v FROM {A} a JOIN {B} b ON a.k_int = b.k_int AND a.k_str = b.k_str", 1)
        .plain());
    qs.add(new Q("left_int_str",
        "SELECT {H} a.k_int, a.k_str, a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_int = b.k_int AND a.k_str = b.k_str",
        1).plain());
    qs.add(new Q("inner_long_dbl",
        "SELECT {H} a.k_long, a.k_dbl, a.v, b.v FROM {A} a JOIN {B} b ON a.k_long = b.k_long AND a.k_dbl = b.k_dbl", 1)
        .plain());
    qs.add(new Q("inner_four_keys",
        "SELECT {H} a.k_int, a.k_long, a.k_str, a.k_dbl, a.v, b.v FROM {A} a JOIN {B} b ON a.k_int = b.k_int "
            + "AND a.k_long = b.k_long AND a.k_str = b.k_str AND a.k_dbl = b.k_dbl", 1).plain());
    qs.add(new Q("left_three_keys_reordered",
        "SELECT {H} a.k_str, a.k_int, a.k_dbl, a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_str = b.k_str "
            + "AND a.k_int = b.k_int AND a.k_dbl = b.k_dbl", 1).plain());
    qs.add(new Q("inner_cross_key_columns",
        "SELECT {H} a.k_int, a.k_str, a.v, b.v FROM {A} a JOIN {B} b ON a.k_int = b.k_long AND a.k_str = b.k_str", 1));
    qs.add(new Q("inner_int_vs_long",
        "SELECT {H} a.k_int, b.k_long, a.v, b.v FROM {A} a JOIN {B} b ON a.k_int = b.k_long", 1));
    // nulls and duplicates
    qs.add(new Q("null_keys_present",
        "SELECT {H} a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_int = b.k_int WHERE a.k_int IS NULL", 1));
    qs.add(new Q("left_anti_pattern",
        "SELECT {H} a.v FROM {A} a LEFT JOIN {B} b ON a.k_int = b.k_int WHERE b.v IS NULL", 1).colocatable());
    qs.add(new Q("hot_key_only",
        "SELECT {H} a.v, b.v FROM {A} a JOIN {B} b ON a.k_int = b.k_int WHERE a.k_int = " + HOT_KEY
            + " AND b.k_int = " + HOT_KEY, 1).colocatable());
    qs.add(new Q("hot_key_count",
        "SELECT {H} COUNT(*) FROM {A} a JOIN {B} b ON a.k_int = b.k_int WHERE a.k_int = " + HOT_KEY, 1).colocatable());
    // empty and no-match
    qs.add(new Q("empty_left",
        "SELECT {H} a.v, b.v FROM (SELECT k_int, v FROM {A} WHERE v < 0) a JOIN {B} b ON a.k_int = b.k_int", 1));
    qs.add(new Q("empty_left_left_join",
        "SELECT {H} a.v, b.v FROM (SELECT k_int, v FROM {A} WHERE v < 0) a LEFT JOIN {B} b ON a.k_int = b.k_int", 1));
    qs.add(new Q("empty_right",
        "SELECT {H} a.v, b.v FROM {A} a JOIN (SELECT k_int, v FROM {B} WHERE v < 0) b ON a.k_int = b.k_int", 1));
    qs.add(new Q("empty_right_left_join",
        "SELECT {H} a.v, b.v FROM {A} a LEFT JOIN (SELECT k_int, v FROM {B} WHERE v < 0) b ON a.k_int = b.k_int", 1));
    // The key sets are disjoint (k_int mod 6 differs) but not provably so for the planner, which would otherwise
    // prune the join to an empty relation.
    qs.add(new Q("no_matches",
        "SELECT {H} a.v, b.v FROM (SELECT k_int, v FROM {A} WHERE k_str = 's0') a JOIN "
            + "(SELECT k_int, v FROM {B} WHERE k_str = 's5') b ON a.k_int = b.k_int", 1));
    qs.add(new Q("no_matches_left_join",
        "SELECT {H} a.v, b.v FROM (SELECT k_int, v FROM {A} WHERE k_str = 's0') a LEFT JOIN "
            + "(SELECT k_int, v FROM {B} WHERE k_str = 's5') b ON a.k_int = b.k_int", 1));
    qs.add(new Q("empty_table_left", "SELECT {H} a.v, b.v FROM te a JOIN {B} b ON a.k_int = b.k_int", 1));
    qs.add(new Q("empty_table_right", "SELECT {H} a.v, b.v FROM {A} a JOIN te b ON a.k_int = b.k_int", 1));
    qs.add(new Q("empty_table_right_left_join",
        "SELECT {H} a.v, b.v FROM {A} a LEFT JOIN te b ON a.k_int = b.k_int", 1));
    // residual non-equi conditions
    qs.add(new Q("inner_residual",
        "SELECT {H} a.k_int, a.v, b.v FROM {A} a JOIN {B} b ON a.k_int = b.k_int AND a.v < b.v", 1).colocatable());
    qs.add(new Q("left_residual_partial",
        "SELECT {H} a.k_int, a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_int = b.k_int AND (a.v + b.v) % 3 = 0", 1)
        .colocatable());
    qs.add(new Q("left_residual_rejects_all",
        "SELECT {H} a.k_int, a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_int = b.k_int AND a.v > b.v + 100000", 1)
        .colocatable());
    qs.add(new Q("left_residual_right_filter",
        "SELECT {H} a.k_int, a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_int = b.k_int AND b.k_str = 's1'", 1));
    // aggregation above the join
    qs.add(new Q("agg_group_by",
        "SELECT {H} a.k_int, COUNT(*), SUM(b.v), MAX(a.v) FROM {A} a JOIN {B} b ON a.k_int = b.k_int "
            + "GROUP BY a.k_int", 1).colocatable());
    qs.add(new Q("agg_left_group_by",
        "SELECT {H} a.k_int, COUNT(*), COUNT(b.v), SUM(b.v) FROM {A} a LEFT JOIN {B} b ON a.k_int = b.k_int "
            + "GROUP BY a.k_int", 1).colocatable());
    qs.add(new Q("agg_global_count",
        "SELECT {H} COUNT(*), SUM(a.v), SUM(b.v) FROM {A} a JOIN {B} b ON a.k_int = b.k_int", 1).colocatable());
    // ORDER BY + LIMIT above the join, including LIMIT below one hot key's product (30 * 25 = 750)
    qs.add(new Q("order_limit_hot_key",
        "SELECT {H} a.v, b.v FROM {A} a JOIN {B} b ON a.k_int = b.k_int WHERE a.k_int = " + HOT_KEY
            + " ORDER BY a.v, b.v LIMIT 10", 1).colocatable().ordered());
    qs.add(new Q("order_limit_one",
        "SELECT {H} a.v, b.v FROM {A} a JOIN {B} b ON a.k_int = b.k_int ORDER BY a.k_int, a.v, b.v LIMIT 1", 1)
        .colocatable().ordered());
    qs.add(new Q("order_limit_offset",
        "SELECT {H} a.k_int, a.v, b.v FROM {A} a JOIN {B} b ON a.k_int = b.k_int ORDER BY a.k_int, a.v, b.v "
            + "LIMIT 25 OFFSET 400", 1).colocatable().ordered());
    qs.add(new Q("order_limit_left",
        "SELECT {H} a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_int = b.k_int ORDER BY a.v, b.v NULLS LAST LIMIT 40", 1)
        .colocatable().ordered());
    qs.add(new Q("order_limit_large",
        "SELECT {H} a.v, b.v FROM {A} a JOIN {B} b ON a.k_int = b.k_int ORDER BY a.v, b.v LIMIT 100000", 1)
        .colocatable().ordered());
    qs.add(new Q("limit_below_hot_key_product",
        "SELECT {H} COUNT(*) FROM (SELECT a.v AS av, b.v AS bv FROM {A} a JOIN {B} b ON a.k_int = b.k_int "
            + "WHERE a.k_int = " + HOT_KEY + " LIMIT 10) x", 1));
    // subqueries with filters and projections, expression join keys
    qs.add(new Q("subqueries",
        "SELECT {H} a.k_int, a.v2, b.s FROM (SELECT k_int, v * 2 AS v2 FROM {A} WHERE v > 10) a JOIN "
            + "(SELECT k_int, UPPER(k_str) AS s FROM {B} WHERE k_dbl > -1) b ON a.k_int = b.k_int", 1));
    qs.add(new Q("subqueries_left",
        "SELECT {H} a.k_int, a.v2, b.s FROM (SELECT k_int, v * 2 AS v2 FROM {A} WHERE v > 10) a LEFT JOIN "
            + "(SELECT k_int, UPPER(k_str) AS s FROM {B} WHERE k_dbl > -1) b ON a.k_int = b.k_int", 1));
    qs.add(new Q("expression_key_plus_zero",
        "SELECT {H} a.k_int, a.v, b.v FROM {A} a JOIN {B} b ON a.k_int + 0 = b.k_int", 1));
    qs.add(new Q("expression_key_left_plus_one",
        "SELECT {H} a.k_int, a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_int + 1 = b.k_int + 1", 1));
    qs.add(new Q("expression_key_upper",
        "SELECT {H} a.k_str, a.v, b.v FROM {A} a JOIN {B} b ON UPPER(a.k_str) = UPPER(b.k_str)", 1));
    // a join key column repeated on one side (LEFT keeps it, an INNER join derives and pushes the implied predicate)
    qs.add(new Q("left_duplicate_left_key",
        "SELECT {H} a.k_int, a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_int = b.k_int AND a.k_int = b.k_long", 1));
    qs.add(new Q("left_duplicate_right_key",
        "SELECT {H} a.k_int, a.v, b.v FROM {A} a LEFT JOIN {B} b ON a.k_int = b.k_int AND a.k_long = b.k_int", 1));
    qs.add(new Q("inner_duplicate_left_key",
        "SELECT {H} a.k_int, a.v, b.v FROM {A} a JOIN {B} b ON a.k_int = b.k_int AND a.k_int = b.k_long", 1));
    // self join and stacked joins
    qs.add(new Q("self_join", "SELECT {H} a.k_int, a.v, b.v FROM {A} a JOIN {A} b ON a.k_int = b.k_int", 1)
        .colocatable().plain());
    qs.add(new Q("self_left_join_multi_key",
        "SELECT {H} a.k_int, a.v, b.v FROM {A} a LEFT JOIN {A} b ON a.k_int = b.k_int AND a.k_str = b.k_str", 1));
    qs.add(new Q("stacked_inner",
        "SELECT {H} a.k_int, a.v, b.v, c.v FROM {A} a JOIN {B} b ON a.k_int = b.k_int JOIN {C} c "
            + "ON b.k_int = c.k_int", 2).colocatable());
    qs.add(new Q("stacked_inner_count",
        "SELECT {H} COUNT(*), SUM(a.v + b.v + c.v) FROM {A} a JOIN {B} b ON a.k_int = b.k_int JOIN {C} c "
            + "ON a.k_int = c.k_int", 2).colocatable());
    qs.add(new Q("stacked_left_then_inner_different_keys",
        "SELECT {H} a.k_int, a.v, b.v, c.v FROM {A} a LEFT JOIN {B} b ON a.k_int = b.k_int JOIN {C} c "
            + "ON b.k_str = c.k_str WHERE a.v < 40", 2));
    qs.add(new Q("stacked_inner_then_left_multi_key",
        "SELECT {H} a.k_int, a.v, b.v, c.v FROM {A} a JOIN {B} b ON a.k_int = b.k_int AND a.k_str = b.k_str "
            + "LEFT JOIN {C} c ON b.k_int = c.k_int AND b.k_str = c.k_str", 2));
    return qs;
  }
  //@formatter:on

  private static Q query(String name) {
    return queries().stream().filter(q -> q._name.equals(name)).findFirst().orElseThrow();
  }

  @DataProvider(name = "cases")
  public Object[][] cases() {
    List<Object[]> cases = new ArrayList<>();
    for (Q q : queries()) {
      for (Shape shape : Shape.values()) {
        if (shape._colocated && !q._colocatable) {
          continue;
        }
        for (Path path : Path.values()) {
          if (path == Path.MERGE_AUTO && !(shape == Shape.OFFLINE && q._plainInputs)) {
            continue;
          }
          cases.add(new Object[]{new Case(q, shape, path)});
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    MockInstanceDataManagerFactory factory1 = new MockInstanceDataManagerFactory("server1");
    MockInstanceDataManagerFactory factory2 = new MockInstanceDataManagerFactory("server2");
    setH2Connection();
    Map<String, Pair<String, List<List<String>>>> partitionedSegmentsMap = new HashMap<>();

    long now = System.currentTimeMillis();
    long offlineTs = now - TimeUnit.DAYS.toMillis(2);
    Schema schema = buildSchema();

    // OFFLINE only: rows spread over 2 servers x 3 segments.
    String[] offlineTables = {"t1", "t2", "t3"};
    int[] sizes = {70, 60, 40};
    int[] hotCounts = {30, 25, 10};
    for (int i = 0; i < offlineTables.length; i++) {
      String offline = offlineTables[i] + "_OFFLINE";
      factory1.registerTable(schema, offline);
      factory2.registerTable(schema, offline);
      addSpread(factory1, factory2, offline, generateRows(1000L + i, sizes[i], hotCounts[i], offlineTs, 0), 3);
    }

    // Empty OFFLINE table (one empty segment on each server).
    factory1.registerTable(schema, "te_OFFLINE");
    factory2.registerTable(schema, "te_OFFLINE");
    factory1.addSegment("te_OFFLINE", new ArrayList<>());
    factory2.addSegment("te_OFFLINE", new ArrayList<>());

    // Hybrid: OFFLINE rows are older than the time boundary (now - 1d), REALTIME rows are newer. Both types are
    // present on both servers, two segments each.
    String[] hybridTables = {"h1", "h2", "h3"};
    for (int i = 0; i < hybridTables.length; i++) {
      String table = hybridTables[i];
      factory1.registerTable(schema, table);
      factory2.registerTable(schema, table);
      List<GenericRow> offlineRows = generateRows(2000L + i, sizes[i] / 2, hotCounts[i] / 2, offlineTs, 0);
      List<GenericRow> realtimeRows =
          generateRows(3000L + i, sizes[i] - sizes[i] / 2, hotCounts[i] - hotCounts[i] / 2, now, 5000);
      addSpread(factory1, factory2, table + "_OFFLINE", offlineRows, 2);
      addSpread(factory1, factory2, table + "_REALTIME", realtimeRows, 2);
    }

    // Hash partitioned on k_int (hashcode, 4 partitions). Partitions 0,1 on server1, 2,3 on server2, 2 segments per
    // partition so that a worker reads multiple segments.
    String[] partitionedTables = {"p1", "p2", "p3"};
    for (int i = 0; i < partitionedTables.length; i++) {
      String offline = partitionedTables[i] + "_OFFLINE";
      factory1.registerTable(schema, offline);
      factory2.registerTable(schema, offline);
      List<GenericRow> rows = generateRows(4000L + i, sizes[i], hotCounts[i], offlineTs, 0);
      List<List<String>> partitionToSegments = new ArrayList<>();
      for (int p = 0; p < NUM_PARTITIONS; p++) {
        partitionToSegments.add(new ArrayList<>());
      }
      for (int round = 0; round < 2; round++) {
        List<List<GenericRow>> partitionRows = new ArrayList<>();
        for (int p = 0; p < NUM_PARTITIONS; p++) {
          partitionRows.add(new ArrayList<>());
        }
        for (int r = round; r < rows.size(); r += 2) {
          GenericRow row = rows.get(r);
          Object key = row.getValue("k_int");
          int partition = key == null ? 0 : (key.hashCode() & Integer.MAX_VALUE) % NUM_PARTITIONS;
          partitionRows.get(partition).add(row);
        }
        for (int p = 0; p < NUM_PARTITIONS; p++) {
          MockInstanceDataManagerFactory factory = p < NUM_PARTITIONS / 2 ? factory1 : factory2;
          partitionToSegments.get(p).add(factory.addSegment(offline, partitionRows.get(p)).getSegmentName());
        }
      }
      partitionedSegmentsMap.put(offline, Pair.of("k_int", partitionToSegments));
    }

    for (String table : new String[]{"t1", "t2", "t3", "te", "h1", "h2", "h3", "p1", "p2", "p3"}) {
      addTableToH2(table, schema);
      addDataToH2(table, schema, factory1.buildTableRowsMap().get(table));
      addDataToH2(table, schema, factory2.buildTableRowsMap().get(table));
    }

    _reducerHostname = "localhost";
    _reducerPort = QueryTestUtils.getAvailablePort();
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    reducerConfig.put(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, _reducerPort);
    _mailboxService =
        new MailboxService(_reducerHostname, _reducerPort, InstanceType.BROKER, new PinotConfiguration(reducerConfig));
    _mailboxService.start();

    QueryServerEnclosure server1 = new QueryServerEnclosure(factory1);
    QueryServerEnclosure server2 = new QueryServerEnclosure(factory2);
    server1.start();
    server2.start();
    int port1 = server1.getPort();
    int port2 = server2.getPort();
    _servers.put(new QueryServerInstance("Server_localhost_" + port1, "localhost", port1, port1), server1);
    _servers.put(new QueryServerInstance("Server_localhost_" + port2, "localhost", port2, port2), server2);

    _gateOnEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerPort, port1, port2,
        factory1.getRegisteredSchemaMap(), factory1.buildTableSegmentNameMap(), factory2.buildTableSegmentNameMap(),
        partitionedSegmentsMap, true);
    _gateOffEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerPort, port1, port2,
        factory1.getRegisteredSchemaMap(), factory1.buildTableSegmentNameMap(), factory2.buildTableSegmentNameMap(),
        partitionedSegmentsMap, false);
    _queryEnvironment = _gateOnEnvironment;
  }

  @AfterClass
  public void tearDown() {
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
  }

  private static Schema buildSchema() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("sortedJoin")
        .addSingleValueDimension("k_int", FieldSpec.DataType.INT)
        .addSingleValueDimension("k_long", FieldSpec.DataType.LONG)
        .addSingleValueDimension("k_str", FieldSpec.DataType.STRING)
        .addSingleValueDimension("k_dbl", FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension("v", FieldSpec.DataType.INT)
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:SECONDS").build();
    schema.setEnableColumnBasedNullHandling(true);
    for (String column : List.of("k_int", "k_long", "k_str", "k_dbl", "v")) {
      schema.getFieldSpecMap().get(column).setNotNull(false);
    }
    return schema;
  }

  /// Rows over a small key domain so keys repeat (many-to-many), with a hot key and NULLs in every key column. `v`
  /// is unique per table (offset by `vBase`) so that result rows are identifiable.
  private static List<GenericRow> generateRows(long seed, int numRows, int hotCount, long ts, int vBase) {
    Random random = new Random(seed);
    List<GenericRow> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      int key = i < hotCount ? HOT_KEY : random.nextInt(15) - 5;
      GenericRow row = new GenericRow();
      row.putValue("k_int", random.nextInt(100) < 8 ? null : key);
      row.putValue("k_long", random.nextInt(100) < 8 ? null : (long) Math.floorMod(key, 5) * 10_000_000_000L);
      row.putValue("k_str", random.nextInt(100) < 8 ? null : "s" + Math.floorMod(key, 6));
      row.putValue("k_dbl", random.nextInt(100) < 8 ? null : Math.floorMod(key, 4) * 0.5 - 1);
      row.putValue("v", vBase + i);
      row.putValue("ts", ts);
      rows.add(row);
    }
    // The hot rows come first in generation order, shuffle so that they are spread over segments and servers.
    Collections.shuffle(rows, random);
    return rows;
  }

  /// Distributes rows over both servers, `segmentsPerServer` segments each.
  private static void addSpread(MockInstanceDataManagerFactory factory1, MockInstanceDataManagerFactory factory2,
      String tableWithType, List<GenericRow> rows, int segmentsPerServer) {
    int numBuckets = 2 * segmentsPerServer;
    List<List<GenericRow>> buckets = new ArrayList<>();
    for (int i = 0; i < numBuckets; i++) {
      buckets.add(new ArrayList<>());
    }
    for (int i = 0; i < rows.size(); i++) {
      buckets.get(i % numBuckets).add(rows.get(i));
    }
    for (int i = 0; i < numBuckets; i++) {
      (i % 2 == 0 ? factory1 : factory2).addSegment(tableWithType, buckets.get(i));
    }
  }

  // --------------------------------------------------------------------------
  // helpers
  // --------------------------------------------------------------------------

  private static String render(Q q, Shape shape, boolean sorted) {
    List<String> options = new ArrayList<>();
    if (sorted) {
      options.add(SORTED_HINT);
    }
    if (shape._colocated) {
      options.add("is_colocated_by_join_keys='true'");
    }
    String hint = options.isEmpty() ? "" : "/*+ joinOptions(" + String.join(", ", options) + ") */";
    String sql = q._sql.replace("{H}", hint).replace("{A}", shape.table(0)).replace("{B}", shape.table(1));
    if (q._needsC) {
      sql = sql.replace("{C}", shape.table(2));
    }
    return sql;
  }

  /// H2 does not understand the hints, strip the hint comments.
  private static String h2Sql(String sql) {
    return sql.replaceAll("/\\*\\+.*?\\*/", "");
  }

  private QueryDispatcher.QueryResult run(String sql, QueryEnvironment environment) {
    _queryEnvironment = environment;
    return queryRunner(sql, true);
  }

  private Map<Integer, DispatchablePlanFragment> plan(String sql, QueryEnvironment environment) {
    _queryEnvironment = environment;
    return planQuery(sql).getQueryPlan().getQueryStageMap();
  }

  private static int countJoins(Map<Integer, DispatchablePlanFragment> stages, JoinNode.JoinStrategy strategy) {
    int count = 0;
    for (DispatchablePlanFragment fragment : stages.values()) {
      count += countJoins(fragment.getPlanFragment().getFragmentRoot(), strategy);
    }
    return count;
  }

  private static int countJoins(PlanNode node, JoinNode.JoinStrategy strategy) {
    int count = node instanceof JoinNode && ((JoinNode) node).getJoinStrategy() == strategy ? 1 : 0;
    for (PlanNode input : node.getInputs()) {
      count += countJoins(input, strategy);
    }
    return count;
  }

  private static List<MailboxMergeReceiveNode> mergeReceives(Map<Integer, DispatchablePlanFragment> stages) {
    List<MailboxMergeReceiveNode> result = new ArrayList<>();
    for (DispatchablePlanFragment fragment : stages.values()) {
      collectMergeReceives(fragment.getPlanFragment().getFragmentRoot(), result);
    }
    return result;
  }

  private static void collectMergeReceives(PlanNode node, List<MailboxMergeReceiveNode> result) {
    if (node instanceof MailboxMergeReceiveNode) {
      result.add((MailboxMergeReceiveNode) node);
    }
    for (PlanNode input : node.getInputs()) {
      collectMergeReceives(input, result);
    }
  }

  /// The contract of the sorted merge join: every input arrives ordered by its join keys, ASC NULLS LAST, in the
  /// order of the keys. With the gate on the input is the merge receive itself; with the gate off it is an unbounded
  /// sort above a plain receive. A wrong direction or null ordering on one side can still produce the right rows
  /// (the operator skips NULL keys wherever they sit), so the ordering is asserted on the plan.
  private static void assertJoinInputOrdering(Map<Integer, DispatchablePlanFragment> stages, boolean gateOn,
      String sql) {
    for (DispatchablePlanFragment fragment : stages.values()) {
      assertJoinInputOrdering(fragment.getPlanFragment().getFragmentRoot(), gateOn, sql);
    }
  }

  private static void assertJoinInputOrdering(PlanNode node, boolean gateOn, String sql) {
    if (node instanceof JoinNode && ((JoinNode) node).getJoinStrategy() == JoinNode.JoinStrategy.SORTED) {
      JoinNode join = (JoinNode) node;
      List<List<Integer>> keys = List.of(join.getLeftKeys(), join.getRightKeys());
      for (int side = 0; side < 2; side++) {
        PlanNode input = join.getInputs().get(side);
        List<RelFieldCollation> collations;
        if (gateOn) {
          Assert.assertTrue(input instanceof MailboxMergeReceiveNode, "join input is " + input.explain() + ": " + sql);
          collations = ((MailboxMergeReceiveNode) input).getCollations();
        } else {
          Assert.assertTrue(input instanceof SortNode, "join input is " + input.explain() + ": " + sql);
          SortNode sort = (SortNode) input;
          Assert.assertEquals(sort.getFetch(), Integer.MAX_VALUE, "the join input sort must be unbounded: " + sql);
          Assert.assertTrue(sort.getOffset() <= 0, "unexpected offset on the join input sort: " + sql);
          PlanNode below = sort.getInputs().get(0);
          Assert.assertTrue(below instanceof MailboxReceiveNode,
              "expected a plain receive under the sort, got " + below.explain() + ": " + sql);
          collations = sort.getCollations();
        }
        List<Integer> sortedFields = new ArrayList<>();
        for (RelFieldCollation collation : collations) {
          sortedFields.add(collation.getFieldIndex());
          Assert.assertEquals(collation.getDirection(), RelFieldCollation.Direction.ASCENDING, sql);
          Assert.assertEquals(collation.nullDirection, RelFieldCollation.NullDirection.LAST,
              "side " + side + " must sort NULLS LAST: " + sql);
        }
        Assert.assertEquals(sortedFields, keys.get(side), "side " + side + " is not sorted on its join keys: " + sql);
      }
    }
    for (PlanNode input : node.getInputs()) {
      assertJoinInputOrdering(input, gateOn, sql);
    }
  }

  private static int countPrePartitionedSends(Map<Integer, DispatchablePlanFragment> stages) {
    int count = 0;
    for (DispatchablePlanFragment fragment : stages.values()) {
      count += countPrePartitionedSends(fragment.getPlanFragment().getFragmentRoot());
    }
    return count;
  }

  private static int countPrePartitionedSends(PlanNode node) {
    int count = node instanceof MailboxSendNode && ((MailboxSendNode) node).isPrePartitioned() ? 1 : 0;
    for (PlanNode input : node.getInputs()) {
      count += countPrePartitionedSends(input);
    }
    return count;
  }

  private static int countOperators(JsonNode node, String type) {
    int count = type.equals(node.path("type").asText()) ? 1 : 0;
    for (JsonNode child : node.path("children")) {
      count += countOperators(child, type);
    }
    return count;
  }

  private ObjectNode stats(Map<Integer, DispatchablePlanFragment> stages, QueryDispatcher.QueryResult result) {
    return new MultiStageStatsTreeBuilder(stages, result.getQueryStats()).jsonStatsByStage(0);
  }

  private static void assertNoError(QueryDispatcher.QueryResult result, String sql) {
    Assert.assertNull(result.getProcessingException(), "Query failed: " + result.getProcessingException() + "\n" + sql);
  }

  /// Runs the case: plan shape, result vs H2, runtime operator types. Returns the SORT_OR_LIMIT operator count.
  private int runCase(Case c)
      throws Exception {
    Q q = c._query;
    Shape shape = c._shape;
    Path path = c._path;
    boolean sorted = path != Path.HASH;
    String baseSql = render(q, shape, sorted);
    String prefix = "";
    if (path == Path.MERGE_PUSHDOWN) {
      prefix = "SET " + QueryOptionKey.SORTED_SELECTION_MERGE_MODE + " = 'ON'; ";
    } else if (path == Path.MERGE_AUTO) {
      prefix = "SET " + QueryOptionKey.SORTED_SELECTION_MERGE_MODE + " = 'AUTO'; ";
    }
    String sql = prefix + baseSql;
    QueryEnvironment environment = path == Path.GATE_OFF ? _gateOffEnvironment : _gateOnEnvironment;

    // Plan shape.
    Map<Integer, DispatchablePlanFragment> stages = plan(sql, environment);
    int expectedSortedJoins = sorted ? q._numJoins : 0;
    Assert.assertEquals(countJoins(stages, JoinNode.JoinStrategy.SORTED), expectedSortedJoins, "sorted joins in plan");
    if (sorted) {
      assertJoinInputOrdering(stages, path != Path.GATE_OFF, sql);
    }
    List<MailboxMergeReceiveNode> mergeReceives = mergeReceives(stages);
    if (sorted && path != Path.GATE_OFF) {
      Assert.assertTrue(mergeReceives.size() >= 2 * q._numJoins,
          "expected a merge receive on every sorted join input, found " + mergeReceives.size() + " for: " + sql);
      if (!shape._colocated && shape != Shape.PARTITIONED && q._plainInputs) {
        // The leaves run on both servers, so the receive merges more than one sender.
        for (MailboxMergeReceiveNode receive : mergeReceives) {
          DispatchablePlanFragment sender = stages.get(receive.getSenderStageId());
          Assert.assertTrue(sender.getServerInstanceToWorkerIdMap().size() >= 2,
              "merge receive of a single-server sender: " + sql);
        }
      }
    } else {
      Assert.assertEquals(mergeReceives.size(), 0, "no merge receive expected for: " + sql);
    }
    if (shape._colocated && sorted) {
      Assert.assertTrue(countPrePartitionedSends(stages) >= 2, "expected pre-partitioned sends for: " + sql);
    }

    // Results.
    QueryDispatcher.QueryResult result = run(sql, environment);
    assertNoError(result, sql);
    compareRowEquals(result.getResultTable(), queryH2(h2Sql(baseSql)), q._ordered);

    // Runtime operators.
    ObjectNode statsTree = stats(stages, result);
    Assert.assertEquals(countOperators(statsTree, "SORTED_MERGE_JOIN"), expectedSortedJoins,
        "SORTED_MERGE_JOIN operators: " + statsTree);
    if (sorted) {
      Assert.assertEquals(countOperators(statsTree, "HASH_JOIN"), 0, "unexpected hash join: " + statsTree);
    } else {
      Assert.assertTrue(countOperators(statsTree, "HASH_JOIN") >= q._numJoins, "hash join missing: " + statsTree);
    }
    return countOperators(statsTree, "SORT_OR_LIMIT");
  }

  // --------------------------------------------------------------------------
  // tests
  // --------------------------------------------------------------------------

  /// The generated data really has what the cases rely on.
  @Test
  public void testDataHasNullsAndHotKey() {
    for (String table : List.of("t1", "t2", "h1", "p1")) {
      for (String column : List.of("k_int", "k_long", "k_str", "k_dbl")) {
        ResultTable result =
            run("SELECT COUNT(*) FROM " + table + " WHERE " + column + " IS NULL", _gateOnEnvironment).getResultTable();
        Assert.assertTrue(((Number) result.getRows().get(0)[0]).longValue() > 0,
            table + "." + column + " has no NULLs");
      }
    }
    ResultTable hot = run("SELECT COUNT(*) FROM t1 a JOIN t2 b ON a.k_int = b.k_int WHERE a.k_int = " + HOT_KEY,
        _gateOnEnvironment).getResultTable();
    Assert.assertTrue(((Number) hot.getRows().get(0)[0]).longValue() > 500, "hot key product too small");
  }

  @Test(dataProvider = "cases")
  public void testCase(Case c)
      throws Exception {
    try {
      runCase(c);
    } catch (AssertionError | RuntimeException e) {
      // Name the case in the failure, the data provider index alone does not say which one it is.
      throw new AssertionError("Case " + c + " failed: " + e.getMessage(), e);
    }
  }

  /// With the gate on and `sortedSelectionMergeMode=ON`, single-type leaves absorb the join-input sort into the V1
  /// request, so there is one fewer SORT_OR_LIMIT per join input; hybrid inputs keep the MSE sort. AUTO pushes the
  /// same way (the server then decides which combine serves the request).
  @Test
  public void testLeafSortPushdownIsEffective()
      throws Exception {
    for (Q q : List.of(query("inner_int"), query("left_str"))) {
      Assert.assertEquals(runCase(new Case(q, Shape.OFFLINE, Path.MERGE)), 2,
          "sender sort of both inputs stays in the MSE by default, " + q);
      Assert.assertEquals(runCase(new Case(q, Shape.OFFLINE, Path.MERGE_PUSHDOWN)), 0,
          "both leaves must absorb the sort with mode ON, " + q);
      // The planner pushes the sender sort for ON and AUTO alike; AUTO only differs in which combine the server picks.
      Assert.assertEquals(runCase(new Case(q, Shape.OFFLINE, Path.MERGE_AUTO)), 0,
          "AUTO pushes the sort like ON, " + q);

      Assert.assertEquals(runCase(new Case(q, Shape.HYBRID, Path.MERGE)), 2);
      Assert.assertEquals(runCase(new Case(q, Shape.HYBRID, Path.MERGE_PUSHDOWN)), 2,
          "hybrid leaves keep the MSE sort, " + q);

      // One single-type input and one hybrid input: only the single-type one is pushed.
      Assert.assertEquals(runCase(new Case(q, Shape.MIXED, Path.MERGE)), 2);
      Assert.assertEquals(runCase(new Case(q, Shape.MIXED, Path.MERGE_PUSHDOWN)), 1,
          "only the OFFLINE input of the mixed join is pushed, " + q);
    }
  }

  /// Gate off keeps an explicit sort above each plain exchange, also when the pushdown option is set.
  @Test
  public void testGateOffSortsAboveExchange()
      throws Exception {
    Q q = query("inner_int");
    Assert.assertEquals(runCase(new Case(q, Shape.OFFLINE, Path.GATE_OFF)), 2,
        "one SORT_OR_LIMIT above each of the 2 join inputs");
    // Without a merge receive there is nothing to push into.
    String baseSql = render(q, Shape.OFFLINE, true);
    String sql = "SET " + QueryOptionKey.SORTED_SELECTION_MERGE_MODE + " = 'ON'; " + baseSql;
    Map<Integer, DispatchablePlanFragment> stages = plan(sql, _gateOffEnvironment);
    QueryDispatcher.QueryResult result = run(sql, _gateOffEnvironment);
    assertNoError(result, sql);
    compareRowEquals(result.getResultTable(), queryH2(h2Sql(baseSql)));
    Assert.assertEquals(countOperators(stats(stages, result), "SORT_OR_LIMIT"), 2);
  }

  /// Sorted and hash join must agree with each other on every query, not only each with H2.
  @Test
  public void testSortedEqualsHash()
      throws Exception {
    for (Q q : queries()) {
      String sortedSql = render(q, Shape.OFFLINE, true);
      String hashSql = render(q, Shape.OFFLINE, false);
      QueryDispatcher.QueryResult sortedResult = run(sortedSql, _gateOnEnvironment);
      QueryDispatcher.QueryResult hashResult = run(hashSql, _gateOnEnvironment);
      assertNoError(sortedResult, sortedSql);
      assertNoError(hashResult, hashSql);
      compareRowEquals(sortedResult.getResultTable(), hashResult.getResultTable().getRows(), q._ordered);
    }
  }

  @Test
  public void testPhysicalOptimizerIsRejected() {
    String sql = "SET usePhysicalOptimizer=true; " + render(query("inner_int"), Shape.OFFLINE, true);
    try {
      QueryDispatcher.QueryResult result = run(sql, _gateOnEnvironment);
      Assert.fail("expected the sorted join with the physical optimizer to be rejected, got: "
          + (result.getProcessingException() != null ? result.getProcessingException() : result.getResultTable()));
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage() != null && e.getMessage().toLowerCase().contains("sorted"),
          "unexpected error: " + e);
    }
  }

  @Test
  public void testUnsupportedJoinShapesAreRejected() {
    String[] sqls = {
        // no equi key
        "SELECT /*+ joinOptions(join_strategy='sorted') */ a.v FROM t1 a JOIN t2 b ON a.v < b.v",
        // distribution hints would break the co-location of keys
        "SELECT /*+ joinOptions(join_strategy='sorted', left_distribution_type='broadcast') */ a.v FROM t1 a "
            + "JOIN t2 b ON a.k_int = b.k_int",
    };
    for (String sql : sqls) {
      try {
        QueryDispatcher.QueryResult result = run(sql, _gateOnEnvironment);
        Assert.fail("expected rejection of: " + sql + ", got: "
            + (result.getProcessingException() != null ? result.getProcessingException() : result.getResultTable()));
      } catch (RuntimeException e) {
        // expected
      }
    }
  }
}
