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
package org.apache.pinot.controller.api.resources;

import io.swagger.annotations.ApiOperation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import javax.ws.rs.core.HttpHeaders;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Guards controller REST methods that are intentionally omitted from Swagger UI
 * ({@link ApiOperation#hidden()} must remain {@code true}).
 */
public class HiddenSwaggerApiOperationsTest {

  @Test
  public void testRiskyOperationsAreHiddenFromSwagger() throws Exception {
    assertHidden(PinotSchemaRestletResource.class, "deleteSchema", String.class, HttpHeaders.class);
    assertHidden(PinotInstanceAssignmentRestletResource.class, "removeInstancePartitions", String.class, String.class,
        HttpHeaders.class);
    assertHidden(PinotTableRestletResource.class, "deleteTable", String.class, String.class, String.class,
        HttpHeaders.class);
    assertHidden(PinotTableRestletResource.class, "cancelRebalance", String.class, String.class, HttpHeaders.class);
    assertHidden(PinotTableRestletResource.class, "deleteTimeBoundary", String.class, HttpHeaders.class);
    assertHidden(PinotAccessControlUserRestletResource.class, "deleteUser", String.class, String.class);
    assertHidden(PinotRunningQueryResource.class, "cancelQuery", String.class, long.class, int.class, boolean.class,
        HttpHeaders.class);
    assertHidden(PinotRunningQueryResource.class, "cancelClientQueryInBroker", String.class, long.class, int.class,
        boolean.class, HttpHeaders.class);
    assertHidden(PinotRunningQueryResource.class, "cancelClientQuery", String.class, int.class, boolean.class,
        HttpHeaders.class);
    assertHidden(PinotDatabaseRestletResource.class, "deleteTablesInDatabase", String.class, boolean.class);
    assertHidden(PinotTaskRestletResource.class, "deleteTaskMetadataByTable", String.class, String.class,
        HttpHeaders.class);
    assertHidden(PinotTaskRestletResource.class, "deleteTasks", String.class, boolean.class);
    assertHidden(PinotTaskRestletResource.class, "deleteTask", String.class, boolean.class);
    assertHidden(ZookeeperResource.class, "delete", String.class);
    assertHidden(PinotTenantRestletResource.class, "deleteTenant", String.class, String.class);
    assertHidden(PinotQueryWorkloadRestletResource.class, "deleteQueryWorkloadConfig", String.class, HttpHeaders.class);
    assertHidden(PinotTableInstances.class, "removeIngestionMetrics", String.class, String.class, Set.class,
        HttpHeaders.class);
    assertHidden(TableConfigsRestletResource.class, "deleteConfig", String.class, HttpHeaders.class);
    assertHidden(PinotClusterConfigs.class, "deleteClusterConfig", String.class);
    assertHidden(PinotInstanceRestletResource.class, "dropInstance", String.class);
    assertHidden(PinotLogicalTableResource.class, "deleteLogicalTable", String.class, HttpHeaders.class);
    assertHidden(PinotSegmentRestletResource.class, "deleteSegment", String.class, String.class, String.class,
        HttpHeaders.class);
    assertHidden(PinotSegmentRestletResource.class, "deleteMultipleSegments", String.class, String.class, String.class,
        List.class, HttpHeaders.class);
    assertHidden(PinotSegmentRestletResource.class, "deleteSegmentsWithTimeWindow", String.class, String.class,
        boolean.class, String.class, String.class, boolean.class, String.class, HttpHeaders.class);
    assertHidden(PinotSegmentRestletResource.class, "deleteSegmentsFromSequenceNum", String.class, List.class,
        boolean.class, boolean.class, HttpHeaders.class);
  }

  private static void assertHidden(Class<?> clazz, String methodName, Class<?>... parameterTypes)
      throws NoSuchMethodException {
    Method method = clazz.getDeclaredMethod(methodName, parameterTypes);
    ApiOperation apiOperation = method.getAnnotation(ApiOperation.class);
    Assert.assertNotNull(apiOperation, "Missing @ApiOperation on " + clazz.getSimpleName() + "." + methodName);
    Assert.assertTrue(apiOperation.hidden(),
        "@ApiOperation.hidden must be true for " + clazz.getSimpleName() + "." + methodName);
  }
}
