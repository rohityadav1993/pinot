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
package org.apache.pinot.tools;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


/**
 * Quickstart that demonstrates the streaming sorted merge join ({@code join_strategy='sorted'}) on a realtime table.
 *
 * <p>It reuses the realtime {@code meetupRsvp} stream from {@link RealtimeQuickStart} (so there is a live mix of
 * CONSUMING and, after the force-commit issued below, ONLINE segments) and runs a self-join on {@code group_id}.
 * Both the hash and sorted join variants are run side by side so their results can be compared. The query runs on the
 * multi-stage engine; the planner inserts a hash-distributed, sorted exchange on each join input so the
 * {@code SortedMergeJoinOperator} can advance both inputs with a two-pointer merge instead of building a hash table.
 *
 * <p>Run with: {@code bin/pinot-admin.sh QuickStart -type SORTED-MERGE-JOIN}
 */
public class SortedMergeJoinQuickStart extends RealtimeQuickStart {
  private static final String TABLE_NAME = "meetupRsvp";

  private static final Map<String, String> USE_MSE =
      Map.of("queryOptions", QueryOptionKey.USE_MULTISTAGE_ENGINE + "=true");

  private static final String HASH_JOIN =
      "SELECT a.group_id, COUNT(*) FROM meetupRsvp a JOIN meetupRsvp b ON a.group_id = b.group_id "
          + "GROUP BY a.group_id ORDER BY a.group_id LIMIT 10";

  private static final String SORTED_JOIN =
      "SELECT /*+ joinOptions(join_strategy='sorted') */ a.group_id, COUNT(*) FROM meetupRsvp a "
          + "JOIN meetupRsvp b ON a.group_id = b.group_id GROUP BY a.group_id ORDER BY a.group_id LIMIT 10";

  @Override
  public List<String> types() {
    return Arrays.asList("SORTED-MERGE-JOIN", "STREAM-SORTED-MERGE-JOIN");
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    // Force-commit the consuming segments so the table holds a mix of ONLINE (committed) and CONSUMING segments.
    forceCommit(TABLE_NAME);
    printStatus(Color.CYAN, "***** Waiting for segments to commit *****");
    Thread.sleep(5000);

    printStatus(Color.YELLOW, "***** Sorted merge join quickstart setup complete *****");

    printStatus(Color.YELLOW, "***** Hash join (default strategy) *****");
    printStatus(Color.CYAN, "Query : " + HASH_JOIN);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(HASH_JOIN, USE_MSE)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.YELLOW, "***** Streaming sorted merge join (join_strategy='sorted') *****");
    printStatus(Color.CYAN, "Query : " + SORTED_JOIN);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(SORTED_JOIN, USE_MSE)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "Example query run completed. The two queries above should return identical results.");
  }

  /**
   * Issues a force-commit on the realtime table so its consuming segments are committed to ONLINE, leaving a mix of
   * ONLINE and freshly-created CONSUMING segments.
   */
  private void forceCommit(String tableName) {
    String urlString =
        "http://localhost:" + QuickstartRunner.DEFAULT_CONTROLLER_PORT + "/tables/" + tableName + "/forceCommit";
    try {
      HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
      connection.setRequestMethod("POST");
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");
      try (OutputStream os = connection.getOutputStream()) {
        os.write(new byte[0]);
      }
      int responseCode = connection.getResponseCode();
      printStatus(Color.CYAN, "***** Force-commit on " + tableName + " returned HTTP " + responseCode + " *****");
      connection.disconnect();
    } catch (IOException e) {
      printStatus(Color.YELLOW, "***** Force-commit request failed (continuing): " + e.getMessage() + " *****");
    }
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "SORTED-MERGE-JOIN"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[0]));
  }
}
