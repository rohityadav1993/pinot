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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.queries.BaseQueriesTest;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.MultiStageStatsTreeBuilder;
import org.apache.pinot.query.runtime.operator.LeafOperator;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/// End-to-end tests of the sorted streaming GROUP BY (`sortedGroupByMode`) in the multi-stage engine, on two servers
/// and several segments per server.
///
/// The same logical events are loaded into two tables, differing only in layout:
/// - [#SPREAD]: every user's events are spread over four segments and both servers, so merging one user's events
///   needs the combine across segments and the exchange across servers (plan shape (a)).
/// - [#EXCLUSIVE]: every user lives on exactly one server, in two segments of it, which is the partition-exclusive
///   layout plan shape (b) (`is_partitioned_by_group_by_keys`) relies on.
///
/// Oracles: H2 for the generic GROUP BY, the existing hash GROUP BY (`sortedGroupByMode` off) plus a by-construction
/// expectation for the ordered funnel and for the attempts query (H2 has no funnel function), and SSE `FUNNEL_COUNT`
/// for the unordered bridge.
///
/// Thread-safety: the tests share the servers and H2 connection and are not meant to run concurrently.
public class SortedStreamingGroupByMseTest extends QueryRunnerTestBase {
  private static final String SPREAD = "fx_spread";
  private static final String EXCLUSIVE = "fx_excl";
  private static final int NUM_USERS = 60;
  /// Users from this index on have all their events after the scanned range, so the funnel query drops them.
  private static final int NUM_USERS_IN_RANGE = 50;
  private static final long WINDOW_MS = 100;
  private static final long BUCKET_MS = 10_000;
  /// Segment creation rejects a time column before 1971.
  private static final long TS_BASE = 1_700_000_000_000L;
  private static final long RANGE_HI = TS_BASE + 1_000 + 3_000L * (NUM_USERS_IN_RANGE - 1);
  /// Two segments per server for the exclusive layout, four segments (two per server) for the spread layout.
  private static final int NUM_SEGMENTS_SPREAD = 4;

  /// The attempts users (ADR 0014) live far after the funnel users, so the ordered-funnel queries (which scan
  /// `[0, RANGE_HI + WINDOW_MS]`) never see them, and the attempts query (which scans `[ATTEMPT_LO, ATTEMPT_HI +
  /// ATTEMPT_CW)`) never sees the funnel users. Both are multiples of [#BUCKET_MS].
  private static final long ATTEMPT_LO = TS_BASE + 1_000_000L;
  private static final int NUM_ATTEMPT_BUCKETS = 5;
  private static final long ATTEMPT_HI = ATTEMPT_LO + NUM_ATTEMPT_BUCKETS * BUCKET_MS;
  private static final long ATTEMPT_CW = 100;
  private static final int FIRST_ATTEMPT_USER = 1_000;

  private static final String GROUP_BY_HINT_B = "/*+ aggOptions(is_partitioned_by_group_by_keys='true') */ ";

  private final List<ImmutableSegment> _server1Segments = new ArrayList<>();
  private final List<ImmutableSegment> _server2Segments = new ArrayList<>();
  private SseOracle _sse;

  /// Expected `[bucket, step1Users, step2Users, step3Users]`, by bucket, built from the scenario of each user.
  private final Map<Long, long[]> _expectedFunnel = new TreeMap<>();
  /// Expected number of users in the scanned range by deepest step (1 to 3), built from the scenario of each user.
  private final Map<Long, Long> _expectedUsersByDepth = new TreeMap<>();

  /// Expected `[bucket, step1Attempts, step2Attempts, step3Attempts]` of the attempts query, by bucket, built from
  /// the hand-specified attempts of every attempts user (see [#addAttemptUsers]).
  private final Map<Long, long[]> _expectedAttempts = new TreeMap<>();
  /// Expected number of attempts by depth (1 to 3), same source.
  private final Map<Long, Long> _expectedAttemptsByDepth = new TreeMap<>();

  private static Schema schema(String name) {
    return new Schema.SchemaBuilder().setSchemaName(name)
        .addSingleValueDimension("correlation_id", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("event_type", FieldSpec.DataType.STRING, "")
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .addSingleValueDimension("bucket", FieldSpec.DataType.LONG, 0L).addMetric("val", FieldSpec.DataType.INT, 0)
        .setEnableColumnBasedNullHandling(true).build();
  }

  /// One event of one user.
  private static final class Event {
    final int _user;
    final String _type;
    final long _ts;
    final int _val;

    Event(int user, String type, long ts, int val) {
      _user = user;
      _type = type;
      _ts = ts;
      _val = val;
    }

    GenericRow toRow() {
      GenericRow row = new GenericRow();
      row.putValue("correlation_id", userId(_user));
      row.putValue("event_type", _type);
      row.putValue("ts", _ts);
      row.putValue("bucket", _ts / BUCKET_MS);
      row.putValue("val", _val);
      return row;
    }
  }

  private static String userId(int user) {
    return String.format("u%03d", user);
  }

  /// Builds the events of every user from its scenario (user index modulo 6; the attempts users come from
  /// [#addAttemptUsers]), and records the expected funnel of the
  /// users in the scanned range. `a`, `b`, `c` are the funnel steps and `x` matches none of them.
  /// - 0: a, b, c within the window (3 steps), plus noise
  /// - 1: a, b (2 steps), plus noise
  /// - 2: a (1 step)
  /// - 3: a, then b far outside the window (1 step)
  /// - 4: a that goes nowhere, then a full a, b, c far later (3 steps, entry is the retry, possibly in a later bucket)
  /// - 5: noise only (0 steps, dropped)
  private List<Event> buildEvents() {
    List<Event> events = new ArrayList<>();
    for (int u = 0; u < NUM_USERS; u++) {
      long t0 = TS_BASE + 1_000 + 3_000L * u;
      int scenario = u % 6;
      int depth;
      long entry;
      int k = 0;
      switch (scenario) {
        case 0:
          events.add(new Event(u, "a", t0, u + k++));
          events.add(new Event(u, "x", t0 + 5, u + k++));
          events.add(new Event(u, "b", t0 + 10, u + k++));
          events.add(new Event(u, "x", t0 + 15, u + k++));
          events.add(new Event(u, "c", t0 + 20, u + k));
          depth = 3;
          entry = t0;
          break;
        case 1:
          events.add(new Event(u, "a", t0, u + k++));
          events.add(new Event(u, "x", t0 + 5, u + k++));
          events.add(new Event(u, "b", t0 + 10, u + k));
          depth = 2;
          entry = t0;
          break;
        case 2:
          events.add(new Event(u, "a", t0, u));
          depth = 1;
          entry = t0;
          break;
        case 3:
          events.add(new Event(u, "a", t0, u + k++));
          events.add(new Event(u, "b", t0 + 500, u + k));
          depth = 1;
          entry = t0;
          break;
        case 4:
          events.add(new Event(u, "a", t0, u + k++));
          events.add(new Event(u, "a", t0 + 1_000, u + k++));
          events.add(new Event(u, "b", t0 + 1_010, u + k++));
          events.add(new Event(u, "c", t0 + 1_020, u + k));
          depth = 3;
          entry = t0 + 1_000;
          break;
        default:
          for (int i = 0; i < 4; i++) {
            events.add(new Event(u, "x", t0 + 10L * i, u + i));
          }
          depth = 0;
          entry = 0;
          break;
      }
      if (u < NUM_USERS_IN_RANGE && depth >= 1) {
        long[] counts = _expectedFunnel.computeIfAbsent(entry / BUCKET_MS, b -> new long[]{b, 0, 0, 0});
        for (int step = 1; step <= depth; step++) {
          counts[step]++;
        }
        _expectedUsersByDepth.merge((long) depth, 1L, Long::sum);
      }
    }
    addAttemptUsers(events);
    return events;
  }

  /// Records one expected attempt `(start, depth)` if the query reports it: starts in `[ATTEMPT_LO, ATTEMPT_HI)`.
  /// The ones outside are scanned (or, before `ATTEMPT_LO`, not) but dropped by the outer filter.
  private void expectAttempt(long start, int depth) {
    if (start < ATTEMPT_LO || start >= ATTEMPT_HI) {
      return;
    }
    long[] counts = _expectedAttempts.computeIfAbsent(start / BUCKET_MS, b -> new long[]{b, 0, 0, 0});
    for (int step = 1; step <= depth; step++) {
      counts[step]++;
    }
    _expectedAttemptsByDepth.merge((long) depth, 1L, Long::sum);
  }

  /// Adds the users of the attempts query (`FUNNEL_ATTEMPTS_ANY_ORDER`, window [#ATTEMPT_CW], steps a, b, c, `x`
  /// matches none) with the attempts each one has by construction. `t` is the user's anchor, offsets are in ms.
  ///
  /// Scenarios 0 to 8 are replicated in every bucket, a different number of times each, so the per-bucket counts
  /// differ. Scenarios 9 to 15 sit at the edges of the scanned range `[ATTEMPT_LO, ATTEMPT_HI + ATTEMPT_CW)`.
  ///
  /// - 0 several attempts: a b c at 0, a b at 200, a at 400 and a b c at 9_500 (next bucket): (0,3) (200,2) (400,1)
  ///   (9_500,3)
  /// - 1 absorbed entry steps: a@0, a@30, a@60, b@70, a@99, c@80: one attempt (0,3)
  /// - 2 out of order: a@0, c@10, b@20: (0,3); steps 2..N count in any order
  /// - 3 event at exactly start + cw: a@0, b@50, c@100: (0,2) (c is outside the attempt and is not an entry step)
  /// - 4 entry step at start + cw opens the next attempt: a@0, b@10, a@100, c@110: (0,2) (100,1)
  /// - 5 no entry step: b, c, b, c: no attempt
  /// - 6 steps before the entry step are ignored: c@-20, b@-10, a@0: (0,1)
  /// - 7 later step 1 inside an attempt does not extend it: a@0, a@90, b@150: (0,1) (150 is past the window of the
  ///   first attempt, and b is not an entry step)
  /// - 8 two attempts in one bucket: a@0, b@5, a@300, b@305, c@310: (0,2) (300,3)
  /// - 9 entry step exactly at `ATTEMPT_LO`: a@lo, b@lo+10: (lo,2)
  /// - 10 lookback is not applied: a@lo-10 (before the scanned range, not seen), a@lo+50, b@lo+60: (lo+50,2)
  /// - 11 attempt just before `hi` reaches into `[hi, hi + cw)`: a@hi-1, b@hi+50, c@hi+98: (hi-1,3)
  /// - 12 the last instant of that attempt: a@hi-1, b@hi+98, c@hi+99 (= start + cw, outside): (hi-1,2)
  /// - 13 attempt starting at `hi`, dropped: a@hi, b@hi+10, c@hi+20: no reported attempt
  /// - 14 attempt starting in `(hi, hi + cw)`, dropped: a@hi+10, b@hi+20: no reported attempt
  /// - 15 an attempt before `hi` and a dropped one after it: a@hi-50, b@hi-40, a@hi+60, b@hi+70: (hi-50,2); the second
  ///   pair is past the first window (ends hi+50) and opens an attempt that is dropped
  private void addAttemptUsers(List<Event> events) {
    int user = FIRST_ATTEMPT_USER;
    for (int bucket = 0; bucket < NUM_ATTEMPT_BUCKETS; bucket++) {
      long t = ATTEMPT_LO + bucket * BUCKET_MS + 1_000;
      for (int scenario = 0; scenario <= 8; scenario++) {
        int copies = 1 + (bucket + scenario) % 3;
        for (int copy = 0; copy < copies; copy++) {
          addAttemptScenario(events, user++, scenario, t + 3L * copy);
        }
      }
    }
    for (int scenario = 9; scenario <= 15; scenario++) {
      addAttemptScenario(events, user++, scenario, ATTEMPT_HI);
    }
  }

  private void addAttemptScenario(List<Event> events, int user, int scenario, long t) {
    long lo = ATTEMPT_LO;
    long hi = ATTEMPT_HI;
    String[] types;
    long[] offsets;
    switch (scenario) {
      case 0:
        types = new String[]{"a", "b", "c", "a", "b", "a", "a", "b", "c"};
        offsets = new long[]{0, 10, 20, 200, 210, 400, 9_500, 9_510, 9_520};
        expectAttempt(t, 3);
        expectAttempt(t + 200, 2);
        expectAttempt(t + 400, 1);
        expectAttempt(t + 9_500, 3);
        break;
      case 1:
        types = new String[]{"a", "a", "a", "b", "a", "c"};
        offsets = new long[]{0, 30, 60, 70, 99, 80};
        expectAttempt(t, 3);
        break;
      case 2:
        types = new String[]{"a", "c", "b"};
        offsets = new long[]{0, 10, 20};
        expectAttempt(t, 3);
        break;
      case 3:
        types = new String[]{"a", "b", "c"};
        offsets = new long[]{0, 50, 100};
        expectAttempt(t, 2);
        break;
      case 4:
        types = new String[]{"a", "b", "a", "c"};
        offsets = new long[]{0, 10, 100, 110};
        expectAttempt(t, 2);
        expectAttempt(t + 100, 1);
        break;
      case 5:
        types = new String[]{"b", "c", "b", "c"};
        offsets = new long[]{0, 10, 20, 30};
        break;
      case 6:
        types = new String[]{"c", "b", "a"};
        offsets = new long[]{-20, -10, 0};
        expectAttempt(t, 1);
        break;
      case 7:
        types = new String[]{"a", "a", "b"};
        offsets = new long[]{0, 90, 150};
        expectAttempt(t, 1);
        break;
      case 8:
        types = new String[]{"a", "b", "a", "b", "c"};
        offsets = new long[]{0, 5, 300, 305, 310};
        expectAttempt(t, 2);
        expectAttempt(t + 300, 3);
        break;
      case 9:
        t = lo;
        types = new String[]{"a", "b"};
        offsets = new long[]{0, 10};
        expectAttempt(lo, 2);
        break;
      case 10:
        t = lo;
        types = new String[]{"a", "a", "b"};
        offsets = new long[]{-10, 50, 60};
        expectAttempt(lo + 50, 2);
        break;
      case 11:
        types = new String[]{"a", "b", "c"};
        offsets = new long[]{-1, 50, 98};
        expectAttempt(hi - 1, 3);
        break;
      case 12:
        types = new String[]{"a", "b", "c"};
        offsets = new long[]{-1, 98, 99};
        expectAttempt(hi - 1, 2);
        break;
      case 13:
        types = new String[]{"a", "b", "c"};
        offsets = new long[]{0, 10, 20};
        expectAttempt(hi, 3);
        break;
      case 14:
        types = new String[]{"a", "b"};
        offsets = new long[]{10, 20};
        expectAttempt(hi + 10, 2);
        break;
      default:
        types = new String[]{"a", "b", "a", "b"};
        offsets = new long[]{-50, -40, 60, 70};
        expectAttempt(hi - 50, 2);
        expectAttempt(hi + 60, 2);
        break;
    }
    for (int i = 0; i < types.length; i++) {
      events.add(new Event(user, types[i], t + offsets[i], user + i));
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    MockInstanceDataManagerFactory factory1 = new MockInstanceDataManagerFactory("server1");
    MockInstanceDataManagerFactory factory2 = new MockInstanceDataManagerFactory("server2");
    for (String table : List.of(SPREAD, EXCLUSIVE)) {
      factory1.registerTable(schema(table), table + "_OFFLINE");
      factory2.registerTable(schema(table), table + "_OFFLINE");
    }

    List<Event> events = buildEvents();

    // SPREAD: the k-th event of user u goes to segment (u + k) % 4; segments 0, 1 are on server 1 and 2, 3 on server 2.
    List<List<GenericRow>> spread = newBuckets(NUM_SEGMENTS_SPREAD);
    Map<Integer, Integer> eventIndex = new HashMap<>();
    // EXCLUSIVE: user u is on server u % 2, the k-th event goes to the (k % 2)-th segment of that server.
    List<List<GenericRow>> exclusive = newBuckets(4);
    for (Event event : events) {
      int k = eventIndex.merge(event._user, 1, Integer::sum) - 1;
      spread.get((event._user + k) % NUM_SEGMENTS_SPREAD).add(event.toRow());
      exclusive.get((event._user % 2) * 2 + (k % 2)).add(event.toRow());
    }
    for (int i = 0; i < NUM_SEGMENTS_SPREAD; i++) {
      MockInstanceDataManagerFactory factory = i < 2 ? factory1 : factory2;
      List<ImmutableSegment> segments = i < 2 ? _server1Segments : _server2Segments;
      segments.add(addSortedSegment(factory, SPREAD, spread.get(i)));
      addSortedSegment(factory, EXCLUSIVE, exclusive.get(i));
    }
    _sse = new SseOracle(
        List.of(new ArrayList<IndexSegment>(_server1Segments), new ArrayList<IndexSegment>(_server2Segments)));

    setH2Connection();
    for (String table : List.of(SPREAD, EXCLUSIVE)) {
      Schema schema = schema(table);
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

    QueryServerEnclosure server1 = new QueryServerEnclosure(factory1, Map.of());
    server1.start();
    QueryServerEnclosure server2 = new QueryServerEnclosure(factory2, Map.of());
    server2.start();
    int port1 = server1.getPort();
    int port2 = server2.getPort();
    _servers.put(new QueryServerInstance("Server_localhost_" + port1, "localhost", port1, port1), server1);
    _servers.put(new QueryServerInstance("Server_localhost_" + port2, "localhost", port2, port2), server2);

    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerPort, port1, port2,
        factory1.getRegisteredSchemaMap(), factory1.buildTableSegmentNameMap(), factory2.buildTableSegmentNameMap(),
        null);
  }

  private static List<List<GenericRow>> newBuckets(int n) {
    List<List<GenericRow>> buckets = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      buckets.add(new ArrayList<>());
    }
    return buckets;
  }

  /// Rows are sorted by the key (then time) so the segment is physically sorted on `correlation_id`, which is what
  /// the sorted streaming GROUP BY needs. The segment is built with null handling and holds no null key, so it is
  /// flagged non-null and engages with null handling on, which is the MSE default.
  private static ImmutableSegment addSortedSegment(MockInstanceDataManagerFactory factory, String table,
      List<GenericRow> rows) {
    rows.sort(Comparator.comparing((GenericRow r) -> (String) r.getValue("correlation_id"))
        .thenComparing(r -> (Long) r.getValue("ts")));
    return factory.addSegment(table + "_OFFLINE", rows);
  }

  @AfterClass
  public void tearDown() {
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
    _sse.shutdownExecutor();
  }

  // --------------------------------------------------------------------------
  // Query building
  // --------------------------------------------------------------------------

  private static String funnelQuery(String table, boolean partitionedByKeys) {
    return "SELECT entry_ts / " + BUCKET_MS + " AS bucket, "
        + "SUM(CASE WHEN max_step >= 1 THEN 1 ELSE 0 END) AS step1_users, "
        + "SUM(CASE WHEN max_step >= 2 THEN 1 ELSE 0 END) AS step2_users, "
        + "SUM(CASE WHEN max_step >= 3 THEN 1 ELSE 0 END) AS step3_users "
        + "FROM (SELECT arrayElementAtLong(r, 1) AS max_step, arrayElementAtLong(r, 2) AS entry_ts "
        + "FROM (SELECT " + (partitionedByKeys ? GROUP_BY_HINT_B : "") + "correlation_id, "
        + "FUNNEL_MAX_STEP_WITH_ENTRY(ts, " + WINDOW_MS + ", 3, event_type = 'a', event_type = 'b', "
        + "event_type = 'c', 'STRICT_ORDER') AS r FROM " + table + " WHERE ts BETWEEN 0 AND " + (RANGE_HI + WINDOW_MS)
        + " GROUP BY correlation_id)) "
        + "WHERE max_step >= 1 AND entry_ts BETWEEN 0 AND " + RANGE_HI + " GROUP BY 1 ORDER BY 1";
  }

  /// Unnests each user's `[maxStep, entryTs]` and counts users by deepest step. The outer filter keeps the step
  /// elements of the users that reached at least step 1 (entry times are far above 3).
  private static String unnestFunnelQuery(String table, boolean partitionedByKeys) {
    return "SELECT u.v AS max_step, COUNT(*) AS users "
        + "FROM (SELECT " + (partitionedByKeys ? GROUP_BY_HINT_B : "") + "correlation_id, "
        + "FUNNEL_MAX_STEP_WITH_ENTRY(ts, " + WINDOW_MS + ", 3, event_type = 'a', event_type = 'b', "
        + "event_type = 'c', 'STRICT_ORDER') AS r FROM " + table + " WHERE ts BETWEEN 0 AND " + (RANGE_HI + WINDOW_MS)
        + " GROUP BY correlation_id) f "
        + "CROSS JOIN UNNEST(f.r) AS u(v) WHERE u.v BETWEEN 1 AND 3 GROUP BY 1";
  }

  /// The canonical attempts query of ADR 0014 section 5: half-open ranges, entries in `[ATTEMPT_LO, ATTEMPT_HI)`,
  /// scan `[ATTEMPT_LO, ATTEMPT_HI + ATTEMPT_CW)`.
  private static String attemptsQuery(String table, boolean partitionedByKeys) {
    return "SELECT funnelAttemptStart(u.a) / " + BUCKET_MS + " AS bucket, "
        + "SUM(CASE WHEN funnelAttemptDepth(u.a) >= 1 THEN 1 ELSE 0 END) AS step1_attempts, "
        + "SUM(CASE WHEN funnelAttemptDepth(u.a) >= 2 THEN 1 ELSE 0 END) AS step2_attempts, "
        + "SUM(CASE WHEN funnelAttemptDepth(u.a) >= 3 THEN 1 ELSE 0 END) AS step3_attempts "
        + "FROM (SELECT " + (partitionedByKeys ? GROUP_BY_HINT_B : "") + "correlation_id, "
        + "FUNNEL_ATTEMPTS_ANY_ORDER(ts, " + ATTEMPT_CW + ", 3, event_type = 'a', event_type = 'b', "
        + "event_type = 'c') AS attempts FROM " + table + " WHERE ts >= " + ATTEMPT_LO + " AND ts < "
        + (ATTEMPT_HI + ATTEMPT_CW) + " GROUP BY correlation_id) f "
        + "CROSS JOIN UNNEST(f.attempts) AS u(a) "
        + "WHERE funnelAttemptStart(u.a) >= " + ATTEMPT_LO + " AND funnelAttemptStart(u.a) < " + ATTEMPT_HI
        + " GROUP BY 1";
  }

  /// Same attempts, counted by depth, to catch a wrong depth that cancels out in the per-step sums.
  private static String attemptsByDepthQuery(String table, boolean partitionedByKeys) {
    return "SELECT funnelAttemptDepth(u.a) AS depth, COUNT(*) AS attempts "
        + "FROM (SELECT " + (partitionedByKeys ? GROUP_BY_HINT_B : "") + "correlation_id, "
        + "FUNNEL_ATTEMPTS_ANY_ORDER(ts, " + ATTEMPT_CW + ", 3, event_type = 'a', event_type = 'b', "
        + "event_type = 'c') AS attempts FROM " + table + " WHERE ts >= " + ATTEMPT_LO + " AND ts < "
        + (ATTEMPT_HI + ATTEMPT_CW) + " GROUP BY correlation_id) f "
        + "CROSS JOIN UNNEST(f.attempts) AS u(a) "
        + "WHERE funnelAttemptStart(u.a) >= " + ATTEMPT_LO + " AND funnelAttemptStart(u.a) < " + ATTEMPT_HI
        + " GROUP BY 1";
  }

  private static String genericQuery(String table, boolean partitionedByKeys) {
    return "SELECT " + (partitionedByKeys ? GROUP_BY_HINT_B : "")
        + "correlation_id, COUNT(*) AS cnt, SUM(val) AS total FROM " + table + " GROUP BY correlation_id";
  }

  private static String options(boolean physicalOptimizer, String sortedGroupByMode) {
    return "SET usePhysicalOptimizer=" + physicalOptimizer + "; SET sortedGroupByMode='" + sortedGroupByMode + "'; ";
  }

  @DataProvider(name = "planShapes")
  public Object[][] planShapes() {
    // {physicalOptimizer, partitionedByKeys}
    return new Object[][]{{false, false}, {true, false}, {false, true}, {true, true}};
  }

  @DataProvider(name = "planners")
  public Object[][] planners() {
    return new Object[][]{{false}, {true}};
  }

  // --------------------------------------------------------------------------
  // Result helpers
  // --------------------------------------------------------------------------

  private QueryDispatcher.QueryResult run(String sql) {
    QueryDispatcher.QueryResult result = queryRunner(sql, false);
    Assert.assertNull(result.getProcessingException(), "Query failed: " + result.getProcessingException());
    return result;
  }

  private static List<List<Long>> longRows(ResultTable table) {
    List<List<Long>> rows = new ArrayList<>();
    for (Object[] row : table.getRows()) {
      List<Long> longs = new ArrayList<>(row.length);
      for (Object value : row) {
        longs.add(((Number) value).longValue());
      }
      rows.add(longs);
    }
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }

  private List<List<Long>> expectedFunnelRows() {
    List<List<Long>> rows = new ArrayList<>();
    for (long[] counts : _expectedFunnel.values()) {
      rows.add(Arrays.asList(counts[0], counts[1], counts[2], counts[3]));
    }
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }

  /// Returns the stats of the first leaf operator of the query, with tracing on.
  private ObjectNode leafStats(String sql) {
    QueryDispatcher.QueryResult queryResult = queryRunner(sql, true);
    Assert.assertNull(queryResult.getProcessingException(), "Query failed: " + queryResult.getProcessingException());
    Map<Integer, DispatchablePlanFragment> planNodes = planQuery(sql).getQueryPlan().getQueryStageMap();
    ObjectNode tree = new MultiStageStatsTreeBuilder(planNodes, queryResult.getQueryStats()).jsonStatsByStage(1);
    ObjectNode leaf = findNodeOfType(tree, "LEAF");
    Assert.assertNotNull(leaf, "expected a LEAF node in " + tree);
    return leaf;
  }

  @Nullable
  private static ObjectNode findNodeOfType(JsonNode node, String type) {
    if (type.equals(node.path("type").asText())) {
      return (ObjectNode) node;
    }
    for (JsonNode child : node.path("children")) {
      ObjectNode found = findNodeOfType(child, type);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private static final String ENGAGED_STAT = LeafOperator.StatKey.SORTED_GROUP_BY_ENGAGED.getStatName();
  private static final String FALLBACK_STAT = LeafOperator.StatKey.SORTED_GROUP_BY_FALLBACK_REASONS.getStatName();

  private void assertEngaged(String sql) {
    ObjectNode leaf = leafStats(sql);
    Assert.assertTrue(leaf.path(ENGAGED_STAT).asBoolean(false),
        "the leaf stage did not report the sorted streaming GROUP BY as engaged: " + leaf);
    Assert.assertNull(leaf.get(FALLBACK_STAT), "unexpected fallback reason: " + leaf);
  }

  // --------------------------------------------------------------------------
  // Canonical funnel
  // --------------------------------------------------------------------------

  /// The oracle itself: the hash GROUP BY (mode off) agrees with the by-construction expectation, so the comparisons
  /// below against it mean something. Both layouts, since the answer must not depend on the layout under (a).
  @Test(dataProvider = "planners")
  public void testFunnelHashBaselineMatchesExpectation(boolean physicalOptimizer) {
    for (String table : List.of(SPREAD, EXCLUSIVE)) {
      QueryDispatcher.QueryResult result = run(options(physicalOptimizer, "OFF") + funnelQuery(table, false));
      Assert.assertEquals(longRows(result.getResultTable()), expectedFunnelRows(), table);
    }
  }

  /// Plan shape (a) on the spread layout: each user's events are in four segments over both servers, so the result
  /// is right only if the combine merges them per segment and the exchange merges them across servers.
  @Test(dataProvider = "planners")
  public void testFunnelPlanShapeAOnSpreadLayout(boolean physicalOptimizer) {
    String sql = options(physicalOptimizer, "ON") + funnelQuery(SPREAD, false);
    Assert.assertEquals(longRows(run(sql).getResultTable()), expectedFunnelRows());
    Assert.assertEquals(longRows(run(options(physicalOptimizer, "OFF") + funnelQuery(SPREAD, false)).getResultTable()),
        longRows(run(sql).getResultTable()));
    assertEngaged(sql);
  }

  /// Plan shapes (a) and (b), both planners, on the partition-exclusive layout.
  @Test(dataProvider = "planShapes")
  public void testFunnelOnExclusiveLayout(boolean physicalOptimizer, boolean partitionedByKeys) {
    String sql = options(physicalOptimizer, "ON") + funnelQuery(EXCLUSIVE, partitionedByKeys);
    List<List<Long>> actual = longRows(run(sql).getResultTable());
    Assert.assertEquals(actual, expectedFunnelRows());
    Assert.assertEquals(actual,
        longRows(run(options(physicalOptimizer, "OFF") + funnelQuery(EXCLUSIVE, partitionedByKeys)).getResultTable()));
    assertEngaged(sql);
  }

  /// Guard on the documented hazard of plan shape (b): it trusts that every key lives on one server. On the spread
  /// layout it is wrong, with the hash GROUP BY just as with the streaming one, because each server finalizes its own
  /// partial funnels. This asserts the wrong answer on purpose: if it ever started matching, the hint would no longer
  /// mean "trust the layout" and the documentation of the option would be stale.
  @Test(dataProvider = "planners")
  public void testFunnelPlanShapeBOnNonExclusiveLayoutIsWrong(boolean physicalOptimizer) {
    List<List<Long>> expected = expectedFunnelRows();
    for (String mode : List.of("OFF", "ON")) {
      List<List<Long>> actual =
          longRows(run(options(physicalOptimizer, mode) + funnelQuery(SPREAD, true)).getResultTable());
      Assert.assertNotEquals(actual, expected,
          "plan shape (b) is documented to be wrong when partitions are not exclusive to one server (mode " + mode
              + ")");
    }
  }

  // --------------------------------------------------------------------------
  // UNNEST over the funnel aggregate (ADR 0014 section 6)
  // --------------------------------------------------------------------------

  private List<List<Long>> expectedUsersByDepthRows() {
    List<List<Long>> rows = new ArrayList<>();
    _expectedUsersByDepth.forEach((depth, users) -> rows.add(Arrays.asList(depth, users)));
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }

  /// `CROSS JOIN UNNEST` over the array an aggregate returns, with the streaming GROUP BY off and on: under (b) on
  /// the exclusive layout, where the aggregate is leaf-final and `UNNEST` runs in the leaf stage, and under (a) on
  /// both layouts, where `UNNEST` runs above the FINAL aggregate. Default planner only: the physical optimizer cannot
  /// plan `UNNEST` (pinned in `UnnestSqlPlannerTest`).
  @Test
  public void testUnnestOverFunnelAggregate() {
    List<List<Long>> expected = expectedUsersByDepthRows();
    Assert.assertEquals(expected.size(), 3, "every depth should be present: " + expected);
    Object[][] cases = {{EXCLUSIVE, true}, {EXCLUSIVE, false}, {SPREAD, false}};
    for (Object[] c : cases) {
      String table = (String) c[0];
      boolean partitionedByKeys = (boolean) c[1];
      for (String mode : List.of("OFF", "ON")) {
        String sql = options(false, mode) + unnestFunnelQuery(table, partitionedByKeys);
        Assert.assertEquals(longRows(run(sql).getResultTable()), expected,
            table + ", hint " + partitionedByKeys + ", mode " + mode);
      }
      assertEngaged(options(false, "ON") + unnestFunnelQuery(table, partitionedByKeys));
    }
  }

  // --------------------------------------------------------------------------
  // Canonical attempts query (ADR 0014 sections 5 and 6)
  // --------------------------------------------------------------------------

  @DataProvider(name = "attemptLayouts")
  public Object[][] attemptLayouts() {
    // {table, partitionedByKeys}: (b) and (a) on the exclusive layout, (a) on the spread layout.
    return new Object[][]{{EXCLUSIVE, true}, {EXCLUSIVE, false}, {SPREAD, false}};
  }

  private List<List<Long>> expectedAttemptRows() {
    List<List<Long>> rows = new ArrayList<>();
    for (long[] counts : _expectedAttempts.values()) {
      rows.add(Arrays.asList(counts[0], counts[1], counts[2], counts[3]));
    }
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }

  private List<List<Long>> expectedAttemptsByDepthRows() {
    List<List<Long>> rows = new ArrayList<>();
    _expectedAttemptsByDepth.forEach((depth, attempts) -> rows.add(Arrays.asList(depth, attempts)));
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }

  /// The expectation is not vacuous: every bucket and every depth is present, and the per-bucket counts differ.
  @Test
  public void testAttemptsExpectationCoversEveryBucketAndDepth() {
    Assert.assertEquals(_expectedAttempts.size(), NUM_ATTEMPT_BUCKETS,
        "expected one entry per bucket of the range: " + _expectedAttempts);
    Assert.assertEquals(_expectedAttemptsByDepth.keySet(), Set.of(1L, 2L, 3L));
    long distinct = _expectedAttempts.values().stream().map(c -> Arrays.toString(Arrays.copyOfRange(c, 1, 4)))
        .distinct().count();
    Assert.assertTrue(distinct > 1, "buckets should not all expect the same counts: " + _expectedAttempts);
  }

  /// The canonical attempts query, sorted streaming GROUP BY on, default planner: plan shape (b) and (a) on the
  /// exclusive layout, plan shape (a) on the spread layout. Matches the by-construction expectation and the result
  /// with the streaming GROUP BY off (the oracle), by step and by depth, and the leaf reports engagement.
  @Test(dataProvider = "attemptLayouts")
  public void testAttemptsQueryMatchesExpectation(String table, boolean partitionedByKeys) {
    String on = options(false, "ON") + attemptsQuery(table, partitionedByKeys);
    String off = options(false, "OFF") + attemptsQuery(table, partitionedByKeys);
    List<List<Long>> actual = longRows(run(on).getResultTable());
    Assert.assertEquals(actual, expectedAttemptRows(), table + ", hint " + partitionedByKeys);
    Assert.assertEquals(longRows(run(off).getResultTable()), actual, "OFF (hash GROUP BY) is the oracle");

    String byDepthOn = options(false, "ON") + attemptsByDepthQuery(table, partitionedByKeys);
    String byDepthOff = options(false, "OFF") + attemptsByDepthQuery(table, partitionedByKeys);
    List<List<Long>> byDepth = longRows(run(byDepthOn).getResultTable());
    Assert.assertEquals(byDepth, expectedAttemptsByDepthRows(), table + ", hint " + partitionedByKeys);
    Assert.assertEquals(longRows(run(byDepthOff).getResultTable()), byDepth);

    assertEngaged(on);
  }

  /// The plan has exactly one leaf stage (the one with the table scan) and one `UNNEST`. Under (b) `UNNEST` runs in the
  /// leaf stage (no array crosses the network); under (a) it runs in a stage above the final aggregate. The leaf is
  /// below the stage that holds the outer aggregate, so its stats are read through the stage 1 tree, as
  /// `assertEngaged` does for every query here.
  @Test(dataProvider = "attemptLayouts")
  public void testAttemptsQueryPlanHasUnnestAndLeafStage(String table, boolean partitionedByKeys) {
    String sql = options(false, "ON") + attemptsQuery(table, partitionedByKeys);
    Map<Integer, DispatchablePlanFragment> stages = planQuery(sql).getQueryPlan().getQueryStageMap();
    List<Integer> leafStages = new ArrayList<>();
    List<Integer> unnestStages = new ArrayList<>();
    stages.forEach((id, fragment) -> {
      PlanNode root = fragment.getPlanFragment().getFragmentRoot();
      if (!findNodes(root, TableScanNode.class).isEmpty()) {
        leafStages.add(id);
      }
      unnestStages.addAll(Collections.nCopies(findNodes(root, UnnestNode.class).size(), id));
    });
    Assert.assertEquals(leafStages.size(), 1, "expected one leaf stage: " + leafStages);
    Assert.assertEquals(unnestStages.size(), 1, "expected one UNNEST: " + unnestStages);
    Assert.assertEquals(unnestStages.get(0).equals(leafStages.get(0)), partitionedByKeys,
        "UNNEST should be in the leaf stage exactly under plan shape (b): leaf " + leafStages + ", UNNEST "
            + unnestStages);
    assertEngaged(sql);
  }

  /// Documented hazard of plan shape (b), as for the ordered funnel: on the spread layout it is not asserted to
  /// match; it is asserted not to, so a change in that behaviour is noticed (and the option docs revisited).
  @Test
  public void testAttemptsPlanShapeBOnNonExclusiveLayoutIsWrong() {
    List<List<Long>> expected = expectedAttemptRows();
    for (String mode : List.of("OFF", "ON")) {
      List<List<Long>> actual =
          longRows(run(options(false, mode) + attemptsQuery(SPREAD, true)).getResultTable());
      Assert.assertNotEquals(actual, expected,
          "plan shape (b) is documented to be wrong when partitions are not exclusive to one server (mode " + mode
              + ")");
    }
  }

  /// Pins current behaviour (ADR 0014 section 6, same guard as `UnnestSqlPlannerTest`): the physical optimizer
  /// cannot plan `UNNEST`, so the attempts query fails at planning. If this starts planning, revisit the docs
  /// ("default planner only").
  @Test(dataProvider = "attemptLayouts")
  public void testAttemptsQueryFailsWithPhysicalOptimizer(String table, boolean partitionedByKeys) {
    String sql = options(true, "ON") + attemptsQuery(table, partitionedByKeys);
    try {
      planQuery(sql);
      Assert.fail("Expected the physical optimizer to reject UNNEST");
    } catch (RuntimeException e) {
      Assert.assertTrue(ExceptionUtils.getStackTrace(e).contains("Unexpected relNode type"),
          "Unexpected error: " + ExceptionUtils.getStackTrace(e));
    }
  }

  private static <T extends PlanNode> List<T> findNodes(PlanNode node, Class<T> type) {
    List<T> nodes = new ArrayList<>();
    if (type.isInstance(node)) {
      nodes.add(type.cast(node));
    }
    for (PlanNode input : node.getInputs()) {
      nodes.addAll(findNodes(input, type));
    }
    return nodes;
  }

  // --------------------------------------------------------------------------
  // Generic GROUP BY on the sorted column
  // --------------------------------------------------------------------------

  @Test(dataProvider = "planShapes")
  public void testGenericGroupByMatchesH2OnExclusiveLayout(boolean physicalOptimizer, boolean partitionedByKeys)
      throws Exception {
    String sql = options(physicalOptimizer, "ON") + genericQuery(EXCLUSIVE, partitionedByKeys);
    compareRowEquals(run(sql).getResultTable(), queryH2(genericQuery(EXCLUSIVE, false)));
    assertEngaged(sql);
  }

  /// Plan shape (a) on the spread layout: a user's COUNT and SUM are partial in each of the four segments.
  @Test(dataProvider = "planners")
  public void testGenericGroupByMatchesH2OnSpreadLayout(boolean physicalOptimizer)
      throws Exception {
    String sql = options(physicalOptimizer, "ON") + genericQuery(SPREAD, false);
    compareRowEquals(run(sql).getResultTable(), queryH2(genericQuery(SPREAD, false)));
    assertEngaged(sql);
  }

  /// Plan shape (b) on the spread layout returns one row per (user, server) instead of one per user.
  @Test(dataProvider = "planners")
  public void testGenericGroupByPlanShapeBOnNonExclusiveLayoutIsWrong(boolean physicalOptimizer)
      throws Exception {
    int expectedRows = queryH2(genericQuery(SPREAD, false)).size();
    int actualRows =
        run(options(physicalOptimizer, "ON") + genericQuery(SPREAD, true)).getResultTable().getRows().size();
    Assert.assertTrue(actualRows > expectedRows,
        "plan shape (b) is documented to emit a user once per server when partitions are not exclusive: expected more"
            + " than " + expectedRows + " rows, got " + actualRows);
  }

  // --------------------------------------------------------------------------
  // Engagement reporting
  // --------------------------------------------------------------------------

  @Test
  public void testLeafDoesNotReportEngagementWhenNotRequested() {
    ObjectNode leaf = leafStats(options(false, "OFF") + genericQuery(EXCLUSIVE, false));
    Assert.assertNull(leaf.get(ENGAGED_STAT), "unexpected engagement report: " + leaf);
    Assert.assertNull(leaf.get(FALLBACK_STAT), "unexpected fallback report: " + leaf);
  }

  /// A requested streaming GROUP BY that does not engage is reported with its reason instead of staying silent.
  @Test(dataProvider = "planners")
  public void testLeafReportsFallbackReason(boolean physicalOptimizer) {
    // Two keys: v1 engages on exactly the sorted column.
    String sql = options(physicalOptimizer, "ON")
        + "SELECT correlation_id, bucket, COUNT(*) FROM " + EXCLUSIVE + " GROUP BY correlation_id, bucket";
    ObjectNode leaf = leafStats(sql);
    Assert.assertNull(leaf.get(ENGAGED_STAT), "unexpected engagement report: " + leaf);
    Assert.assertTrue(leaf.path(FALLBACK_STAT).toString().contains("multiple group-by expressions"),
        "expected the fallback reason in " + leaf);
  }

  // --------------------------------------------------------------------------
  // Unordered bridge query against SSE FUNNEL_COUNT
  // --------------------------------------------------------------------------

  /// ADR 0005 Decision 9: the unordered semantics expressed with the generic GROUP BY (per-step flags, the event
  /// bucket in the inner GROUP BY) agree with SSE `FUNNEL_COUNT` on the same data. SSE uses the default exact `set`
  /// strategy (`partitioned` is wrong across segments, and `partitioned_by_correlate_by` is not on this branch). The
  /// inner GROUP BY has two keys, so it runs on the hash GROUP BY here and the streaming option only has to be
  /// harmless; it reports the fallback.
  @Test(dataProvider = "planners")
  public void testUnorderedBridgeMatchesSseFunnelCount(boolean physicalOptimizer) {
    String bridge = "SELECT bucket, SUM(s1) AS c1, SUM(s1 * s2) AS c2, SUM(s1 * s2 * s3) AS c3 FROM ("
        + "SELECT correlation_id, bucket, "
        + "MAX(CASE WHEN event_type = 'a' THEN 1 ELSE 0 END) AS s1, "
        + "MAX(CASE WHEN event_type = 'b' THEN 1 ELSE 0 END) AS s2, "
        + "MAX(CASE WHEN event_type = 'c' THEN 1 ELSE 0 END) AS s3 FROM " + SPREAD
        + " GROUP BY correlation_id, bucket) GROUP BY bucket ORDER BY bucket";
    String sql = options(physicalOptimizer, "ON") + bridge;
    List<List<Long>> actual = longRows(run(sql).getResultTable());
    Assert.assertFalse(actual.isEmpty());

    BrokerResponseNative response = _sse.query("SELECT bucket, FUNNEL_COUNT(STEPS(event_type = 'a', event_type = 'b', "
        + "event_type = 'c'), CORRELATE_BY(correlation_id)) FROM " + SPREAD + " GROUP BY bucket ORDER BY bucket "
        + "LIMIT 1000");
    List<List<Long>> expected = new ArrayList<>();
    for (Object[] row : response.getResultTable().getRows()) {
      long[] counts = toLongs(row[1]);
      expected.add(Arrays.asList(((Number) row[0]).longValue(), counts[0], counts[1], counts[2]));
    }
    expected.sort(Comparator.comparing(Object::toString));
    Assert.assertEquals(actual, expected);
  }

  private static long[] toLongs(Object value) {
    if (value instanceof long[]) {
      return (long[]) value;
    }
    if (value instanceof int[]) {
      return Arrays.stream((int[]) value).asLongStream().toArray();
    }
    Object[] objects = (Object[]) value;
    return Arrays.stream(objects).mapToLong(o -> ((Number) o).longValue()).toArray();
  }

  /// Runs SSE queries over two "servers", each with the given segments, through the same server and broker reduce
  /// code the SSE tests use.
  private static final class SseOracle extends BaseQueriesTest {
    private final List<List<IndexSegment>> _instances;

    SseOracle(List<List<IndexSegment>> instances) {
      _instances = instances;
    }

    BrokerResponseNative query(String sql) {
      return getBrokerResponse(sql);
    }

    @Override
    protected String getFilter() {
      return "";
    }

    @Override
    protected IndexSegment getIndexSegment() {
      return _instances.get(0).get(0);
    }

    @Override
    protected List<IndexSegment> getIndexSegments() {
      List<IndexSegment> all = new ArrayList<>();
      _instances.forEach(all::addAll);
      return all;
    }

    @Override
    protected List<List<IndexSegment>> getDistinctInstances() {
      return _instances;
    }
  }
}
