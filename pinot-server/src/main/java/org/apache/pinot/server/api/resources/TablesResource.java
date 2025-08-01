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
package org.apache.pinot.server.api.resources;

import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataUtils;
import org.apache.pinot.common.response.server.TableIndexMetadataResponse;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.restlet.resources.SegmentConsumerInfo;
import org.apache.pinot.common.restlet.resources.ServerSegmentsReloadCheckResponse;
import org.apache.pinot.common.restlet.resources.TableLLCSegmentUploadResponse;
import org.apache.pinot.common.restlet.resources.TableMetadataInfo;
import org.apache.pinot.common.restlet.resources.TableSegmentValidationInfo;
import org.apache.pinot.common.restlet.resources.TableSegments;
import org.apache.pinot.common.restlet.resources.TablesList;
import org.apache.pinot.common.restlet.resources.ValidDocIdsBitmapResponse;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentUploader;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.StaleSegment;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.server.access.AccessControlFactory;
import org.apache.pinot.server.api.AdminApiApplication;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = "Table", authorizations = {
    @Authorization(value = CommonConstants.SWAGGER_AUTHORIZATION_KEY), @Authorization(value = CommonConstants.DATABASE)
})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key =
        CommonConstants.SWAGGER_AUTHORIZATION_KEY, description = "The format of the key is  ```\"Basic <token>\" or "
        + "\"Bearer <token>\"```"), @ApiKeyAuthDefinition(name = CommonConstants.DATABASE, in =
    ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = CommonConstants.DATABASE, description =
    "Database context passed through http header. If no context is provided 'default' database "
        + "context will be considered.")
}))
@Path("/")
public class TablesResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(TablesResource.class);
  private static final String PEER_SEGMENT_DOWNLOAD_DIR = "peerSegmentDownloadDir";
  private static final String SEGMENT_UPLOAD_DIR = "segmentUploadDir";

  @Inject
  private ServerInstance _serverInstance;

  @Inject
  private AccessControlFactory _accessControlFactory;

  @Inject
  @Named(AdminApiApplication.SERVER_INSTANCE_ID)
  private String _instanceId;

  @GET
  @Path("/tables")
  @Produces(MediaType.APPLICATION_JSON)
  //swagger annotations
  @ApiOperation(value = "List tables", notes = "List all the tables on this server")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = TablesList.class),
      @ApiResponse(code = 500, message = "Server initialization error", response = ErrorInfo.class)
  })
  public String listTables() {
    InstanceDataManager instanceDataManager = ServerResourceUtils.checkGetInstanceDataManager(_serverInstance);
    List<String> tables = new ArrayList<>(instanceDataManager.getAllTables());
    return ResourceUtils.convertToJsonString(new TablesList(tables));
  }

  @GET
  @Path("/tables/{tableName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List table segments", notes = "List segments of table hosted on this server")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = TableSegments.class),
      @ApiResponse(code = 500, message = "Server initialization error", response = ErrorInfo.class)
  })
  public String listTableSegments(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_OFFLINE")
      @PathParam("tableName") String tableName, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableDataManager tableDataManager = ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableName);
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      List<String> segments = new ArrayList<>(segmentDataManagers.size());
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        segments.add(segmentDataManager.getSegmentName());
      }
      return ResourceUtils.convertToJsonString(new TableSegments(segments));
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }

  @GET
  @Encoded
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/metadata")
  @ApiOperation(value = "List metadata for all segments of a given table", notes = "List segments metadata of table "
      + "hosted on this server")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Table not found")
  })
  public String getSegmentMetadata(
      @ApiParam(value = "Table Name with type", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Column name", allowMultiple = true) @QueryParam("columns") @DefaultValue("")
      List<String> columns, @Context HttpHeaders headers)
      throws WebApplicationException {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();

    if (instanceDataManager == null) {
      throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
    }

    TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableName);
    if (tableDataManager == null) {
      throw new WebApplicationException("Table: " + tableName + " is not found", Response.Status.NOT_FOUND);
    }

    List<String> decodedColumns = new ArrayList<>(columns.size());
    for (String column : columns) {
      decodedColumns.add(URIUtils.decode(column));
    }

    boolean allColumns = false;
    // For robustness, loop over all columns, if any of the columns is "*", return metadata for all columns.
    for (String column : decodedColumns) {
      if (column.equals("*")) {
        allColumns = true;
        break;
      }
    }
    Set<String> columnSet = allColumns ? null : new HashSet<>(decodedColumns);

    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    long totalSegmentSizeBytes = 0;
    long totalNumRows = 0;
    Map<String, Double> columnLengthMap = new HashMap<>();
    Map<String, Double> columnCardinalityMap = new HashMap<>();
    Map<String, Double> maxNumMultiValuesMap = new HashMap<>();
    Map<String, Map<String, Double>> columnIndexSizesMap = new HashMap<>();
    try {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        if (segmentDataManager instanceof ImmutableSegmentDataManager) {
          ImmutableSegment immutableSegment = (ImmutableSegment) segmentDataManager.getSegment();
          long segmentSizeBytes = immutableSegment.getSegmentSizeBytes();
          SegmentMetadataImpl segmentMetadata =
              (SegmentMetadataImpl) segmentDataManager.getSegment().getSegmentMetadata();

          totalSegmentSizeBytes += segmentSizeBytes;
          totalNumRows += segmentMetadata.getTotalDocs();

          if (columnSet == null) {
            columnSet = segmentMetadata.getAllColumns();
          } else {
            columnSet.retainAll(segmentMetadata.getAllColumns());
          }
          for (String column : columnSet) {
            ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataMap().get(column);
            int columnLength = 0;
            DataType storedDataType = columnMetadata.getDataType().getStoredType();
            if (storedDataType.isFixedWidth()) {
              // For type of fixed width: INT, LONG, FLOAT, DOUBLE, BOOLEAN (stored as INT), TIMESTAMP (stored as LONG),
              // set the columnLength as the fixed width.
              columnLength = storedDataType.size();
            } else if (columnMetadata.hasDictionary()) {
              // For type of variable width (String, Bytes), if it's stored using dictionary encoding, set the
              // columnLength as the max length in dictionary.
              columnLength = columnMetadata.getColumnMaxLength();
            } else if (storedDataType == DataType.STRING || storedDataType == DataType.BYTES) {
              // For type of variable width (String, Bytes), if it's stored using raw bytes, set the columnLength as
              // the length of the max value.
              if (columnMetadata.getMaxValue() != null) {
                String maxValueString = (String) columnMetadata.getMaxValue();
                columnLength = maxValueString.getBytes(StandardCharsets.UTF_8).length;
              }
            } else {
              // For type of STRUCT, MAP, LIST, set the columnLength as DEFAULT_MAX_LENGTH (512).
              columnLength = FieldSpec.DEFAULT_MAX_LENGTH;
            }
            int columnCardinality = columnMetadata.getCardinality();
            columnLengthMap.merge(column, (double) columnLength, Double::sum);
            columnCardinalityMap.merge(column, (double) columnCardinality, Double::sum);
            if (!columnMetadata.isSingleValue()) {
              int maxNumMultiValues = columnMetadata.getMaxNumberOfMultiValues();
              maxNumMultiValuesMap.merge(column, (double) maxNumMultiValues, Double::sum);
            }

            IndexService indexService = IndexService.getInstance();
            for (int i = 0, n = columnMetadata.getNumIndexes(); i < n; i++) {
              String indexName = indexService.get(columnMetadata.getIndexType(i)).getId();
              long value = columnMetadata.getIndexSize(i);

              Map<String, Double> columnIndexSizes = columnIndexSizesMap.getOrDefault(column, new HashMap<>());
              Double indexSize = columnIndexSizes.getOrDefault(indexName, 0d) + value;
              columnIndexSizes.put(indexName, indexSize);
              columnIndexSizesMap.put(column, columnIndexSizes);
            }
          }
        }
      }
    } finally {
      // we could release segmentDataManagers as we iterate in the loop above
      // but this is cleaner with clear semantics of usage. Also, above loop
      // executes fast so duration of holding segments is not a concern
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }

    // fetch partition to primary key count for realtime tables that have upsert enabled
    Map<Integer, Long> upsertPartitionToPrimaryKeyCountMap = new HashMap<>();
    if (tableDataManager instanceof RealtimeTableDataManager) {
      RealtimeTableDataManager realtimeTableDataManager = (RealtimeTableDataManager) tableDataManager;
      upsertPartitionToPrimaryKeyCountMap = realtimeTableDataManager.getUpsertPartitionToPrimaryKeyCount();
    }

    // construct upsertPartitionToServerPrimaryKeyCountMap to populate in TableMetadataInfo
    Map<Integer, Map<String, Long>> upsertPartitionToServerPrimaryKeyCountMap = new HashMap<>();
    upsertPartitionToPrimaryKeyCountMap.forEach(
        (partition, primaryKeyCount) -> upsertPartitionToServerPrimaryKeyCountMap.put(partition,
            Map.of(instanceDataManager.getInstanceId(), primaryKeyCount)));

    TableMetadataInfo tableMetadataInfo =
        new TableMetadataInfo(tableDataManager.getTableName(), totalSegmentSizeBytes, segmentDataManagers.size(),
            totalNumRows, columnLengthMap, columnCardinalityMap, maxNumMultiValuesMap, columnIndexSizesMap,
            upsertPartitionToServerPrimaryKeyCountMap);
    return ResourceUtils.convertToJsonString(tableMetadataInfo);
  }

  @GET
  @Encoded
  @Path("/tables/{tableName}/indexes")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Provide index metadata", notes = "Provide index details for the table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error",
      response = ErrorInfo.class), @ApiResponse(code = 404, message = "Table or segment not found", response =
      ErrorInfo.class)
  })
  public String getTableIndexes(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_OFFLINE")
      @PathParam("tableName") String tableName, @Context HttpHeaders headers)
      throws Exception {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableDataManager tableDataManager = ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableName);
    List<SegmentDataManager> allSegments = tableDataManager.acquireAllSegments();
    try {
      int totalSegmentCount = 0;
      Map<String, Map<String, Integer>> columnToIndexesCount = new HashMap<>();
      for (SegmentDataManager segmentDataManager : allSegments) {
        if (segmentDataManager instanceof RealtimeSegmentDataManager) {
          // REALTIME segments may not have indexes since not all indexes have mutable implementations
          continue;
        }
        totalSegmentCount++;
        IndexSegment segment = segmentDataManager.getSegment();
        segment.getColumnNames().forEach(col -> {
          columnToIndexesCount.putIfAbsent(col, new HashMap<>());
          DataSource colDataSource = segment.getDataSource(col);
          IndexService.getInstance().getAllIndexes().forEach(idxType -> {
            int count = colDataSource.getIndex(idxType) != null ? 1 : 0;
            columnToIndexesCount.get(col).merge(idxType.getId(), count, Integer::sum);
          });
        });
      }
      TableIndexMetadataResponse tableIndexMetadataResponse =
          new TableIndexMetadataResponse(totalSegmentCount, columnToIndexesCount);
      return JsonUtils.objectToString(tableIndexMetadataResponse);
    } finally {
      for (SegmentDataManager segmentDataManager : allSegments) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }

  @GET
  @Encoded
  @Path("/tables/{tableName}/segments/{segmentName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Provide segment metadata", notes = "Provide segments metadata for the segment on server")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error", response = ErrorInfo.class),
      @ApiResponse(code = 404, message = "Table or segment not found", response = ErrorInfo.class)
  })
  public String getSegmentMetadata(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_OFFLINE")
      @PathParam("tableName") String tableName,
      @ApiParam(value = "Segment name", required = true) @PathParam("segmentName") String segmentName,
      @ApiParam(value = "Column name", allowMultiple = true) @QueryParam("columns") @DefaultValue("")
      List<String> columns, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    for (int i = 0; i < columns.size(); i++) {
      columns.set(i, URIUtils.decode(columns.get(i)));
    }

    TableDataManager tableDataManager = ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableName);
    segmentName = URIUtils.decode(segmentName);
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      throw new WebApplicationException(String.format("Table %s segments %s does not exist", tableName, segmentName),
          Response.Status.NOT_FOUND);
    }

    try {
      return SegmentMetadataFetcher.getSegmentMetadata(segmentDataManager, columns);
    } catch (Exception e) {
      LOGGER.error("Failed to convert table {} segment {} to json", tableName, segmentName);
      throw new WebApplicationException("Failed to convert segment metadata to json",
          Response.Status.INTERNAL_SERVER_ERROR);
    } finally {
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  @GET
  @Path("/tables/{tableName}/segments/crc")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Provide segment crc information", notes = "Provide crc information for the segments on server")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error", response = ErrorInfo.class),
      @ApiResponse(code = 404, message = "Table or segment not found", response = ErrorInfo.class)
  })
  public String getCrcMetadataForTable(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_OFFLINE")
      @PathParam("tableName") String tableName, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableDataManager tableDataManager = ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableName);
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      Map<String, String> segmentCrcForTable = new HashMap<>();
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        segmentCrcForTable.put(segmentDataManager.getSegmentName(),
            segmentDataManager.getSegment().getSegmentMetadata().getCrc());
      }
      return ResourceUtils.convertToJsonString(segmentCrcForTable);
    } catch (Exception e) {
      throw new WebApplicationException("Failed to convert crc information to json",
          Response.Status.INTERNAL_SERVER_ERROR);
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }

  // TODO Add access control similar to PinotSegmentUploadDownloadRestletResource for segment download.
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Path("/segments/{tableNameWithType}/{segmentName}")
  @ApiOperation(value = "Download an immutable segment", notes = "Download an immutable segment in zipped tar format.")
  public Response downloadSegment(
      @ApiParam(value = "Name of the table with type REALTIME OR OFFLINE", required = true, example = "myTable_OFFLINE")
      @PathParam("tableNameWithType") String tableNameWithType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @Context HttpHeaders httpHeaders)
      throws Exception {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, httpHeaders);
    LOGGER.info("Received a request to download segment {} for table {}", segmentName, tableNameWithType);
    // Validate data access
    ServerResourceUtils.validateDataAccess(_accessControlFactory, tableNameWithType, httpHeaders);

    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      throw new WebApplicationException(
          String.format("Table %s segment %s does not exist", tableNameWithType, segmentName),
          Response.Status.NOT_FOUND);
    }
    try {
      // TODO Limit the number of concurrent downloads of segments because compression is an expensive operation.
      // Store the tar.gz segment file in the server's segmentTarDir folder with a unique file name.
      // Note that two clients asking the same segment file will result in the same tar.gz files being created twice.
      // Will revisit for optimization if performance becomes an issue.
      File tmpSegmentTarDir =
          new File(_serverInstance.getInstanceDataManager().getSegmentFileDirectory(), PEER_SEGMENT_DOWNLOAD_DIR);
      tmpSegmentTarDir.mkdir();

      File segmentTarFile = org.apache.pinot.common.utils.FileUtils.concatAndValidateFile(tmpSegmentTarDir,
          tableNameWithType + "_" + segmentName + "_" + UUID.randomUUID() + TarCompressionUtils.TAR_GZ_FILE_EXTENSION,
          "Invalid table / segment name: %s , %s", tableNameWithType, segmentName);

      TarCompressionUtils.createCompressedTarFile(new File(tableDataManager.getTableDataDir(), segmentName),
          segmentTarFile);
      Response.ResponseBuilder builder = Response.ok();
      builder.entity((StreamingOutput) output -> {
        try {
          Files.copy(segmentTarFile.toPath(), output);
        } finally {
          FileUtils.deleteQuietly(segmentTarFile);
        }
      });
      builder.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + segmentTarFile.getName());
      builder.header(HttpHeaders.CONTENT_LENGTH, segmentTarFile.length());
      return builder.build();
    } finally {
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableNameWithType}/{segmentName}/validDocIdsBitmap")
  @ApiOperation(value = "Download validDocIds bitmap for an REALTIME immutable segment", notes =
      "Download validDocIds for " + "an immutable segment in bitmap format.")
  public ValidDocIdsBitmapResponse downloadValidDocIdsBitmap(
      @ApiParam(value = "Name of the table with type REALTIME", required = true, example = "myTable_REALTIME")
      @PathParam("tableNameWithType") String tableNameWithType,
      @ApiParam(value = "Valid doc ids type") @QueryParam("validDocIdsType") String validDocIdsType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @Context HttpHeaders httpHeaders) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, httpHeaders);
    segmentName = URIUtils.decode(segmentName);
    LOGGER.info("Received a request to download validDocIds for segment {} table {}", segmentName, tableNameWithType);
    // Validate data access
    ServerResourceUtils.validateDataAccess(_accessControlFactory, tableNameWithType, httpHeaders);

    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      throw new WebApplicationException(
          String.format("Table %s segment %s does not exist", tableNameWithType, segmentName),
          Response.Status.NOT_FOUND);
    }

    try {
      IndexSegment indexSegment = segmentDataManager.getSegment();
      if (!(indexSegment instanceof ImmutableSegmentImpl)) {
        throw new WebApplicationException(
            String.format("Table %s segment %s is not a immutable segment", tableNameWithType, segmentName),
            Response.Status.BAD_REQUEST);
      }
      ServiceStatus.Status status = ServiceStatus.getServiceStatus(_instanceId);

      final Pair<ValidDocIdsType, MutableRoaringBitmap> validDocIdsSnapshotPair =
          getValidDocIds(indexSegment, validDocIdsType);
      ValidDocIdsType finalValidDocIdsType = validDocIdsSnapshotPair.getLeft();
      MutableRoaringBitmap validDocIdSnapshot = validDocIdsSnapshotPair.getRight();

      if (validDocIdSnapshot == null) {
        String msg = String.format(
            "Found that validDocIds is missing while fetching validDocIds for table %s segment %s while "
                + "reading the validDocIds with validDocIdType %s", tableNameWithType,
            segmentDataManager.getSegmentName(), validDocIdsType);
        LOGGER.warn(msg);
        throw new WebApplicationException(msg, Response.Status.NOT_FOUND);
      }
      byte[] validDocIdsBytes = RoaringBitmapUtils.serialize(validDocIdSnapshot);
      return new ValidDocIdsBitmapResponse(segmentName, indexSegment.getSegmentMetadata().getCrc(),
          finalValidDocIdsType, validDocIdsBytes, _serverInstance.getInstanceDataManager().getInstanceId(),
          status);
    } finally {
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  /**
   * Download snapshot for the given immutable segment for upsert table. This endpoint is used when get snapshot from
   * peer to avoid recompute when reload segments.
   */
  @Deprecated
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Path("/segments/{tableNameWithType}/{segmentName}/validDocIds")
  @ApiOperation(value = "Download validDocIds for an REALTIME immutable segment", notes = "Download validDocIds for "
      + "an immutable segment in bitmap format.")
  public Response downloadValidDocIds(
      @ApiParam(value = "Name of the table with type REALTIME", required = true, example = "myTable_REALTIME")
      @PathParam("tableNameWithType") String tableNameWithType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "Valid doc ids type")
      @QueryParam("validDocIdsType") String validDocIdsType, @Context HttpHeaders httpHeaders) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, httpHeaders);
    segmentName = URIUtils.decode(segmentName);
    LOGGER.info("Received a request to download validDocIds for segment {} table {}", segmentName, tableNameWithType);
    // Validate data access
    ServerResourceUtils.validateDataAccess(_accessControlFactory, tableNameWithType, httpHeaders);

    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      throw new WebApplicationException(
          String.format("Table %s segment %s does not exist", tableNameWithType, segmentName),
          Response.Status.NOT_FOUND);
    }

    try {
      IndexSegment indexSegment = segmentDataManager.getSegment();
      if (!(indexSegment instanceof ImmutableSegmentImpl)) {
        throw new WebApplicationException(
            String.format("Table %s segment %s is not a immutable segment", tableNameWithType, segmentName),
            Response.Status.BAD_REQUEST);
      }

      final Pair<ValidDocIdsType, MutableRoaringBitmap> validDocIdSnapshotPair =
          getValidDocIds(indexSegment, validDocIdsType);
      MutableRoaringBitmap validDocIdSnapshot = validDocIdSnapshotPair.getRight();
      if (validDocIdSnapshot == null) {
        String msg = String.format(
            "Found that validDocIds is missing while fetching validDocIds for table %s segment %s while "
                + "reading the validDocIds with validDocIdType %s",
            tableNameWithType, segmentDataManager.getSegmentName(), validDocIdsType);
        LOGGER.warn(msg);
        throw new WebApplicationException(msg, Response.Status.NOT_FOUND);
      }

      byte[] validDocIdsBytes = RoaringBitmapUtils.serialize(validDocIdSnapshot);
      Response.ResponseBuilder builder = Response.ok(validDocIdsBytes);
      builder.header(HttpHeaders.CONTENT_LENGTH, validDocIdsBytes.length);
      return builder.build();
    } finally {
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  @Deprecated
  @GET
  @Path("/tables/{tableNameWithType}/validDocIdMetadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Provides segment validDocId metadata", notes = "Provides segment validDocId metadata")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error",
      response = ErrorInfo.class), @ApiResponse(code = 404, message = "Table or segment not found", response =
      ErrorInfo.class)
  })
  public String getValidDocIdsMetadata(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_REALTIME")
      @PathParam("tableNameWithType") String tableNameWithType,
      @ApiParam(value = "Valid doc ids type")
      @QueryParam("validDocIdsType") String validDocIdsType,
      @ApiParam(value = "Segment name", allowMultiple = true) @QueryParam("segmentNames") List<String> segmentNames,
      @Context HttpHeaders headers)
      throws Exception {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    return ResourceUtils.convertToJsonString(
        processValidDocIdsMetadata(tableNameWithType, segmentNames, validDocIdsType));
  }

  @POST
  @Path("/tables/{tableNameWithType}/validDocIdsMetadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Provides segment validDocIds metadata", notes = "Provides segment validDocIds metadata")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error",
      response = ErrorInfo.class), @ApiResponse(code = 404, message = "Table or segment not found", response =
      ErrorInfo.class)
  })
  public String getValidDocIdsMetadata(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_REALTIME")
      @PathParam("tableNameWithType") String tableNameWithType,
      @ApiParam(value = "Valid doc ids type") @QueryParam("validDocIdsType") String validDocIdsType,
      TableSegments tableSegments, @Context HttpHeaders headers) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    List<String> segmentNames = tableSegments.getSegments();
    return ResourceUtils.convertToJsonString(
        processValidDocIdsMetadata(tableNameWithType, segmentNames, validDocIdsType));
  }

  private List<Map<String, Object>> processValidDocIdsMetadata(String tableNameWithType, List<String> segments,
      String validDocIdsType) {
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    List<String> missingSegments = new ArrayList<>();
    int nonImmutableSegmentCount = 0;
    List<String> missingValidDocsSegments = new ArrayList<>();
    List<SegmentDataManager> segmentDataManagers;
    if (segments == null) {
      segmentDataManagers = tableDataManager.acquireAllSegments();
    } else {
      segmentDataManagers = tableDataManager.acquireSegments(segments, missingSegments);
    }
    try {
      if (!missingSegments.isEmpty()) {
        // we need not abort here or throw exception as we can still process the segments that are available
        // During UpsertCompactionTaskGenerator, controller sends a lot of segments to server to fetch validDocIds
        // and it may happen that a segment is deleted concurrently. In such cases, we should log a warning and
        // process the remaining available segments.
        LOGGER.warn("Table {} has missing segments {}", tableNameWithType, missingSegments);
      }
      ServiceStatus.Status status = ServiceStatus.getServiceStatus(_instanceId);

      List<Map<String, Object>> allValidDocIdsMetadata = new ArrayList<>(segmentDataManagers.size());
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        IndexSegment indexSegment = segmentDataManager.getSegment();
        if (indexSegment == null) {
          LOGGER.warn("Table {} segment {} does not exist", tableNameWithType, segmentDataManager.getSegmentName());
          continue;
        }
        // Skip the consuming segments
        if (!(indexSegment instanceof ImmutableSegmentImpl)) {
          if (LOGGER.isDebugEnabled()) {
            String msg = String.format("Table %s segment %s is not a immutable segment", tableNameWithType,
                segmentDataManager.getSegmentName());
            LOGGER.debug(msg);
          }
          nonImmutableSegmentCount++;
          continue;
        }

        final Pair<ValidDocIdsType, MutableRoaringBitmap> validDocIdSnapshotPair =
            getValidDocIds(indexSegment, validDocIdsType);
        String finalValidDocIdsType = validDocIdSnapshotPair.getLeft().toString();
        MutableRoaringBitmap validDocIdsSnapshot = validDocIdSnapshotPair.getRight();
        if (validDocIdsSnapshot == null) {
          if (LOGGER.isDebugEnabled()) {
            String msg = String.format(
                "Found that validDocIds is missing while processing validDocIdsMetadata for table %s segment %s while "
                    + "reading the validDocIds with validDocIdType %s", tableNameWithType,
                segmentDataManager.getSegmentName(), validDocIdsType);
            LOGGER.debug(msg);
          }
          missingValidDocsSegments.add(segmentDataManager.getSegmentName());
          continue;
        }

        Map<String, Object> validDocIdsMetadata = new HashMap<>();
        int totalDocs = indexSegment.getSegmentMetadata().getTotalDocs();
        int totalValidDocs = validDocIdsSnapshot.getCardinality();
        int totalInvalidDocs = totalDocs - totalValidDocs;
        validDocIdsMetadata.put("segmentName", segmentDataManager.getSegmentName());
        validDocIdsMetadata.put("totalDocs", totalDocs);
        validDocIdsMetadata.put("totalValidDocs", totalValidDocs);
        validDocIdsMetadata.put("totalInvalidDocs", totalInvalidDocs);
        validDocIdsMetadata.put("segmentCrc", indexSegment.getSegmentMetadata().getCrc());
        validDocIdsMetadata.put("validDocIdsType", finalValidDocIdsType);
        validDocIdsMetadata.put("serverStatus", status);
        validDocIdsMetadata.put("instanceId", _serverInstance.getInstanceDataManager().getInstanceId());
        if (segmentDataManager instanceof ImmutableSegmentDataManager) {
          validDocIdsMetadata.put("segmentSizeInBytes",
              ((ImmutableSegment) segmentDataManager.getSegment()).getSegmentSizeBytes());
        }
        validDocIdsMetadata.put("segmentCreationTimeMillis", indexSegment.getSegmentMetadata().getIndexCreationTime());
        allValidDocIdsMetadata.add(validDocIdsMetadata);
      }
      if (nonImmutableSegmentCount > 0) {
        LOGGER.warn("Table {} has {} non-immutable segments found while processing validDocIdsMetadata",
            tableNameWithType, nonImmutableSegmentCount);
      }
      if (!missingValidDocsSegments.isEmpty()) {
        LOGGER.warn("Found that validDocIds is missing for segments {} while processing validDocIdsMetadata "
                + "for table {} while reading the validDocIds with validDocIdType {}. ", missingValidDocsSegments,
            tableNameWithType, validDocIdsType);
      }
      return allValidDocIdsMetadata;
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }

  private Pair<ValidDocIdsType, MutableRoaringBitmap> getValidDocIds(IndexSegment indexSegment,
      String validDocIdsTypeStr) {
    if (validDocIdsTypeStr == null) {
      // By default, we read the valid doc ids from snapshot.
      return Pair.of(ValidDocIdsType.SNAPSHOT, ((ImmutableSegmentImpl) indexSegment).loadValidDocIdsFromSnapshot());
    }
    ValidDocIdsType validDocIdsType = ValidDocIdsType.valueOf(validDocIdsTypeStr.toUpperCase());
    switch (validDocIdsType) {
      case SNAPSHOT:
        return Pair.of(validDocIdsType, ((ImmutableSegmentImpl) indexSegment).loadValidDocIdsFromSnapshot());
      case IN_MEMORY:
        return Pair.of(validDocIdsType, indexSegment.getValidDocIds().getMutableRoaringBitmap());
      case IN_MEMORY_WITH_DELETE:
        return Pair.of(validDocIdsType, indexSegment.getQueryableDocIds().getMutableRoaringBitmap());
      default:
        // By default, we read the valid doc ids from snapshot.
        LOGGER.warn("Invalid validDocIdsType: {}. Using default validDocIdsType: {}", validDocIdsType,
            ValidDocIdsType.SNAPSHOT);
        return Pair.of(ValidDocIdsType.SNAPSHOT, ((ImmutableSegmentImpl) indexSegment).loadValidDocIdsFromSnapshot());
    }
  }

  /**
   * Deprecated. Use /segments/{realtimeTableName}/{segmentName}/uploadLLCSegment instead.
   * Upload a low level consumer segment to segment store and return the segment download url. This endpoint is used
   * when segment store copy is unavailable for committed low level consumer segments.
   * Please note that invocation of this endpoint may cause query performance to suffer, since we tar up the segment
   * to upload it.
   *
   * @see <a href="https://tinyurl.com/f63ru4sb></a>
   * @param realtimeTableName table name with type.
   * @param segmentName name of the segment to be uploaded
   * @param timeoutMs timeout for the segment upload to the deep-store. If this is negative, the default timeout
   *                  would be used.
   * @return full url where the segment is uploaded
   * @throws Exception if an error occurred during the segment upload.
   */
  @Deprecated
  @POST
  @Path("/segments/{realtimeTableName}/{segmentName}/upload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Upload a low level consumer segment to segment store and return the segment download url",
      notes = "Upload a low level consumer segment to segment store and return the segment download url")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error", response = ErrorInfo.class),
      @ApiResponse(code = 404, message = "Table or segment not found", response = ErrorInfo.class),
      @ApiResponse(code = 400, message = "Bad request", response = ErrorInfo.class)
  })
  public String uploadLLCSegment(
      @ApiParam(value = "Name of the REALTIME table", required = true) @PathParam("realtimeTableName")
      String realtimeTableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") String segmentName,
      @QueryParam("uploadTimeoutMs") @DefaultValue("-1") int timeoutMs,
      @Context HttpHeaders headers)
      throws Exception {
    realtimeTableName = DatabaseUtils.translateTableName(realtimeTableName, headers);
    LOGGER.info("Received a request to upload low level consumer segment {} for table {}", segmentName,
        realtimeTableName);

    // Check it's realtime table
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(realtimeTableName);
    if (TableType.OFFLINE == tableType) {
      throw new WebApplicationException(
          String.format("Cannot upload low level consumer segment for OFFLINE table: %s", realtimeTableName),
          Response.Status.BAD_REQUEST);
    }

    // Check the segment is low level consumer segment
    if (!LLCSegmentName.isLLCSegment(segmentName)) {
      throw new WebApplicationException(String.format("Segment %s is not a low level consumer segment", segmentName),
          Response.Status.BAD_REQUEST);
    }

    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(realtimeTableName);
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      throw new WebApplicationException(
          String.format("Table %s segment %s does not exist", realtimeTableName, segmentName),
          Response.Status.NOT_FOUND);
    }

    File segmentTarFile = null;
    try {
      segmentTarFile = createSegmentTarFile(tableDataManager, segmentName);
      return uploadSegment(segmentTarFile, tableNameWithType, segmentName, timeoutMs);
    } finally {
      FileUtils.deleteQuietly(segmentTarFile);
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  /**
   * Upload a low level consumer segment to segment store and return the segment download url, crc and
   * other segment metadata. This endpoint is used when segment store copy is unavailable for committed
   * low level consumer segments.
   * Please note that invocation of this endpoint may cause query performance to suffer, since we tar up the segment
   * to upload it.
   *
   * @see <a href="https://tinyurl.com/f63ru4sb></a>
   * @param realtimeTableNameWithType table name with type.
   * @param segmentName name of the segment to be uploaded
   * @param timeoutMs timeout for the segment upload to the deep-store. If this is negative, the default timeout
   *                  would be used.
   * @return full url where the segment is uploaded, crc, segmentName. Can add more segment metadata in the future.
   * @throws Exception if an error occurred during the segment upload.
   */
  @Deprecated
  @POST
  @Path("/segments/{realtimeTableNameWithType}/{segmentName}/uploadLLCSegment")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Upload a low level consumer segment to segment store and return the segment download url,"
      + "crc and other segment metadata",
      notes = "Upload a low level consumer segment to segment store and return the segment download url, crc "
          + "and other segment metadata")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error", response = ErrorInfo.class),
      @ApiResponse(code = 404, message = "Table or segment not found", response = ErrorInfo.class),
      @ApiResponse(code = 400, message = "Bad request", response = ErrorInfo.class)
  })
  public TableLLCSegmentUploadResponse uploadLLCSegmentV2(
      @ApiParam(value = "Name of the REALTIME table", required = true) @PathParam("realtimeTableNameWithType")
      String realtimeTableNameWithType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") String segmentName,
      @QueryParam("uploadTimeoutMs") @DefaultValue("-1") int timeoutMs,
      @Context HttpHeaders headers)
      throws Exception {
    realtimeTableNameWithType = DatabaseUtils.translateTableName(realtimeTableNameWithType, headers);
    LOGGER.info("Received a request to upload low level consumer segment {} for table {}", segmentName,
        realtimeTableNameWithType);

    // Check it's realtime table
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(realtimeTableNameWithType);
    if (TableType.REALTIME != tableType) {
      throw new WebApplicationException(
          String.format("Cannot upload low level consumer segment for a non-realtime table: %s",
              realtimeTableNameWithType), Response.Status.BAD_REQUEST);
    }

    // Check the segment is low level consumer segment
    if (!LLCSegmentName.isLLCSegment(segmentName)) {
      throw new WebApplicationException(String.format("Segment %s is not a low level consumer segment", segmentName),
          Response.Status.BAD_REQUEST);
    }

    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, realtimeTableNameWithType);
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      throw new WebApplicationException(
          String.format("Table %s segment %s does not exist", realtimeTableNameWithType, segmentName),
          Response.Status.NOT_FOUND);
    }

    File segmentTarFile = null;
    try {
      segmentTarFile = createSegmentTarFile(tableDataManager, segmentName);
      String downloadUrl = uploadSegment(segmentTarFile, realtimeTableNameWithType, segmentName, timeoutMs);
      return new TableLLCSegmentUploadResponse(segmentName,
          Long.parseLong(segmentDataManager.getSegment().getSegmentMetadata().getCrc()), downloadUrl);
    } finally {
      FileUtils.deleteQuietly(segmentTarFile);
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  /**
   * Upload a real-time committed segment to segment store and return the segment ZK metadata in json format.
   * This endpoint is used when segment store copy is unavailable for real-time committed segments.
   * Please note that invocation of this endpoint may cause query performance to suffer, since we tar up the segment to
   * upload it.
   *
   * @see <a href="https://tinyurl.com/f63ru4sb></a>
   * @param realtimeTableName table name with type.
   * @param segmentName name of the segment to be uploaded
   * @param timeoutMs timeout for the segment upload to the deep-store. If this is negative, the default timeout
   *                  would be used.
   * @return segment ZK metadata in json format.
   * @throws Exception if an error occurred during the segment upload.
   */
  @POST
  @Path("/segments/{realtimeTableName}/{segmentName}/uploadCommittedSegment")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Upload a real-time committed segment to segment store and return the segment ZK metadata",
      notes = "Upload a real-time committed segment to segment store and return the segment ZK metadata")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad request", response = ErrorInfo.class),
      @ApiResponse(code = 404, message = "Table or segment not found", response = ErrorInfo.class),
      @ApiResponse(code = 500, message = "Internal server error", response = ErrorInfo.class)
  })
  public String uploadCommittedSegment(
      @ApiParam(value = "Name of the real-time table", required = true) @PathParam("realtimeTableName")
      String realtimeTableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") String segmentName,
      @QueryParam("uploadTimeoutMs") @DefaultValue("-1") int timeoutMs, @Context HttpHeaders headers)
      throws Exception {
    realtimeTableName = DatabaseUtils.translateTableName(realtimeTableName, headers);
    LOGGER.info("Received a request to upload committed segment: {} for table: {}", segmentName, realtimeTableName);

    // Check it's real-time table
    if (!TableNameBuilder.isRealtimeTableResource(realtimeTableName)) {
      throw new WebApplicationException(
          "Cannot upload committed segment for a non-realtime table: " + realtimeTableName,
          Response.Status.BAD_REQUEST);
    }

    // Check the segment is low level consumer segment
    if (!LLCSegmentName.isLLCSegment(segmentName)) {
      throw new WebApplicationException(String.format("Segment: %s is not a low level consumer segment", segmentName),
          Response.Status.BAD_REQUEST);
    }

    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, realtimeTableName);
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      throw new WebApplicationException(
          String.format("Failed to find table: %s, segment: %s", realtimeTableName, segmentName),
          Response.Status.NOT_FOUND);
    }
    if (!(segmentDataManager instanceof ImmutableSegmentDataManager)) {
      throw new WebApplicationException(
          String.format("Table: %s, segment: %s hasn't been sealed", realtimeTableName, segmentName),
          Response.Status.NOT_FOUND);
    }

    File segmentTarFile = null;
    try {
      segmentTarFile = createSegmentTarFile(tableDataManager, segmentName);
      String downloadUrl = uploadSegment(segmentTarFile, realtimeTableName, segmentName, timeoutMs);

      // Fetch existing segment ZK Metadata
      SegmentZKMetadata segmentZKMetadata =
          ZKMetadataProvider.getSegmentZKMetadata(_serverInstance.getHelixManager().getHelixPropertyStore(),
              realtimeTableName, segmentName);
      Preconditions.checkState(segmentZKMetadata != null,
          "Failed to find segment ZK metadata for table: %s, segment: %s", realtimeTableName, segmentName);

      // Update the Segment ZK Metadata with the segment metadata present on the server
      ImmutableSegment segment = ((ImmutableSegmentDataManager) segmentDataManager).getSegment();
      SegmentZKMetadataUtils.updateCommittingSegmentZKMetadata(realtimeTableName, segmentZKMetadata,
          segment.getSegmentMetadata(), downloadUrl, segment.getSegmentSizeBytes(), segmentZKMetadata.getEndOffset());
      return segmentZKMetadata.toJsonString();
    } finally {
      FileUtils.deleteQuietly(segmentTarFile);
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  /**
   * Creates a tar.gz segment file in the server's segmentTarUploadDir folder with a unique file name.
   */
  private File createSegmentTarFile(TableDataManager tableDataManager, String segmentName)
      throws IOException {
    File segmentTarUploadDir =
        new File(_serverInstance.getInstanceDataManager().getSegmentFileDirectory(), SEGMENT_UPLOAD_DIR);
    segmentTarUploadDir.mkdir();
    String tableNameWithType = tableDataManager.getTableName();
    File segmentTarFile = org.apache.pinot.common.utils.FileUtils.concatAndValidateFile(segmentTarUploadDir,
        tableNameWithType + "_" + segmentName + "_" + UUID.randomUUID() + TarCompressionUtils.TAR_GZ_FILE_EXTENSION,
        "Invalid table / segment name: %s, %s", tableNameWithType, segmentName);
    TarCompressionUtils.createCompressedTarFile(new File(tableDataManager.getTableDataDir(), segmentName),
        segmentTarFile);
    return segmentTarFile;
  }

  /**
   * Uploads a segment tar file to the segment store and returns the segment download url.
   */
  private String uploadSegment(File segmentTarFile, String tableNameWithType, String segmentName, int timeoutMs) {
    SegmentUploader segmentUploader = _serverInstance.getInstanceDataManager().getSegmentUploader();
    URI segmentDownloadUrl;
    if (timeoutMs <= 0) {
      // Use default timeout if passed timeout is not positive
      segmentDownloadUrl = segmentUploader.uploadSegment(segmentTarFile, new LLCSegmentName(segmentName));
    } else {
      segmentDownloadUrl = segmentUploader.uploadSegment(segmentTarFile, new LLCSegmentName(segmentName), timeoutMs);
    }
    if (segmentDownloadUrl == null) {
      throw new WebApplicationException(
          String.format("Failed to upload table: %s, segment: %s to segment store", tableNameWithType, segmentName),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return segmentDownloadUrl.toString();
  }

  @GET
  @Path("tables/{realtimeTableName}/consumingSegmentsInfo")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the info for consumers of this REALTIME table", notes =
      "Get consumers info from the table data manager. Note that the partitionToOffsetMap has been deprecated "
          + "and will be removed in the next release. The info is now embedded within each partition's state as "
          + "currentOffsetsMap")
  public List<SegmentConsumerInfo> getConsumingSegmentsInfo(
      @ApiParam(value = "Name of the REALTIME table", required = true) @PathParam("realtimeTableName")
      String realtimeTableName, @Context HttpHeaders headers) {
    realtimeTableName = DatabaseUtils.translateTableName(realtimeTableName, headers);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(realtimeTableName);
    if (TableType.OFFLINE == tableType) {
      throw new WebApplicationException("Cannot get consuming segment info for OFFLINE table: " + realtimeTableName);
    }
    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(realtimeTableName);

    List<SegmentConsumerInfo> segmentConsumerInfoList = new ArrayList<>();
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        if (segmentDataManager instanceof RealtimeSegmentDataManager) {
          RealtimeSegmentDataManager realtimeSegmentDataManager = (RealtimeSegmentDataManager) segmentDataManager;
          Map<String, ConsumerPartitionState> partitionStateMap =
              realtimeSegmentDataManager.getConsumerPartitionState();
          Map<String, String> recordsLagMap = new HashMap<>();
          Map<String, String> availabilityLagMsMap = new HashMap<>();
          realtimeSegmentDataManager.getPartitionToLagState(partitionStateMap).forEach((k, v) -> {
            recordsLagMap.put(k, v.getRecordsLag());
            availabilityLagMsMap.put(k, v.getAvailabilityLagMs());
          });
          @Deprecated
          Map<String, String> partitiionToOffsetMap = realtimeSegmentDataManager.getPartitionToCurrentOffset();
          segmentConsumerInfoList.add(new SegmentConsumerInfo(segmentDataManager.getSegmentName(),
              realtimeSegmentDataManager.getConsumerState().toString(),
              realtimeSegmentDataManager.getLastConsumedTimestamp(), partitiionToOffsetMap,
              new SegmentConsumerInfo.PartitionOffsetInfo(partitiionToOffsetMap, partitionStateMap.entrySet().stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getUpstreamLatestOffset().toString())),
                  recordsLagMap, availabilityLagMsMap)));
        }
      }
    } catch (Exception e) {
      throw new WebApplicationException("Caught exception when getting consumer info for table: " + realtimeTableName);
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
    return segmentConsumerInfoList;
  }

  @GET
  @Path("tables/{tableNameWithType}/allSegmentsLoaded")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validates if the ideal state matches with the segment state on this server", notes =
      "Validates if the ideal state matches with the segment state on this server")
  public TableSegmentValidationInfo validateTableSegmentState(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableNameWithType")
      String tableNameWithType, @Context HttpHeaders headers) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    // Get table current ideal state
    IdealState tableIdealState = HelixHelper.getTableIdealState(_serverInstance.getHelixManager(), tableNameWithType);
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);

    // Validate segments in ideal state which belong to this server
    long maxEndTimeMs = -1;
    Map<String, Map<String, String>> instanceStatesMap = tableIdealState.getRecord().getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : instanceStatesMap.entrySet()) {
      String segmentState = entry.getValue().get(_instanceId);
      if (segmentState != null) {
        // Segment hosted by this server. Validate segment state
        String segmentName = entry.getKey();
        SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
        try {
          switch (segmentState) {
            case SegmentStateModel.CONSUMING:
              // Only validate presence of segment
              if (segmentDataManager == null) {
                String invalidReason = String.format(
                    "Segment %s is in CONSUMING state, but segmentDataManager is null", segmentName);
                return new TableSegmentValidationInfo(false, invalidReason, -1);
              }
              break;
            case SegmentStateModel.ONLINE:
              // Validate segment CRC
              SegmentZKMetadata zkMetadata =
                  ZKMetadataProvider.getSegmentZKMetadata(_serverInstance.getHelixManager().getHelixPropertyStore(),
                      tableNameWithType, segmentName);
              Preconditions.checkState(zkMetadata != null,
                  "Segment zk metadata not found for segment : " + segmentName);
              if (segmentDataManager == null) {
                String invalidReason = String.format(
                    "Segment %s is in ONLINE state, but segmentDataManager is null", segmentName);
                return new TableSegmentValidationInfo(false, invalidReason, -1);
              } else if (!segmentDataManager.getSegment().getSegmentMetadata().getCrc()
                  .equals(String.valueOf(zkMetadata.getCrc()))) {
                String invalidReason = String.format(
                    "Segment %s is in ONLINE state, but has CRC mismatch. "
                        + "zk_metadata_crc=%s, segment_data_manager_crc=%s",
                    segmentName, zkMetadata.getCrc(), segmentDataManager.getSegment().getSegmentMetadata().getCrc());
                return new TableSegmentValidationInfo(false, invalidReason, -1);
              }
              maxEndTimeMs = Math.max(maxEndTimeMs, zkMetadata.getEndTimeMs());
              break;
            default:
              break;
          }
        } finally {
          if (segmentDataManager != null) {
            tableDataManager.releaseSegment(segmentDataManager);
          }
        }
      }
    }
    return new TableSegmentValidationInfo(true, null, maxEndTimeMs);
  }

  @GET
  @Path("/tables/{tableName}/segments/needReload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Checks if reload is needed on any segment", notes = "Returns true if reload is required on"
      + " any segment in this server")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = TableSegments.class), @ApiResponse(code = 500,
      message = "Internal Server error", response = ErrorInfo.class)
  })
  public String checkSegmentsReload(
      @ApiParam(value = "Table Name with type", required = true) @PathParam("tableName") String tableName,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableDataManager tableDataManager = ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableName);
    boolean needReload = false;
    try {
      needReload = tableDataManager.needReloadSegments();
    } catch (Exception e) {
      throw new WebApplicationException(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
    }
    return ResourceUtils.convertToJsonString(
        new ServerSegmentsReloadCheckResponse(needReload, tableDataManager.getInstanceId()));
  }

  @GET
  @Path("/tables/{tableName}/segments/isStale")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the list of segments that are stale or deviated from table config.",
      notes = "Get the list of segments that are stale or deviated from table config")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500,
      message = "Internal Server error", response = ErrorInfo.class)
  })
  public List<StaleSegment> getStaleSegments(
      @ApiParam(value = "Table Name with type", required = true) @PathParam("tableName") String tableName,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableDataManager tableDataManager = ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableName);
    try {
      return tableDataManager.getStaleSegments();
    } catch (Exception e) {
      throw new WebApplicationException(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @DELETE
  @Path("/tables/{tableName}/ingestionMetrics")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Remove ingestion metrics for partition(s)", notes = "Removes ingestion-related metrics for "
      + "the given table. If no partitionId is provided, metrics for all partitions hosted by this server will be "
      + "removed.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully removed ingestion metrics"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  public String removeIngestionMetrics(
      @ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName,
      @Nullable @ApiParam(value = "List of partition Ids (optional)") @QueryParam("partitionId")
      Set<Integer> partitionIds,
      @Context HttpHeaders headers) {
    try {
      tableName = DatabaseUtils.translateTableName(tableName, headers);
    } catch (Exception e) {
      throw new WebApplicationException(e.getMessage(), Response.Status.BAD_REQUEST);
    }
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    try {
      if (tableDataManager instanceof RealtimeTableDataManager) {
        RealtimeTableDataManager realtimeTableDataManager = (RealtimeTableDataManager) tableDataManager;
        Set<Integer> removedPartitionIds = realtimeTableDataManager.stopTrackingPartitionIngestionDelay(partitionIds);
        return "Successfully removed ingestion metrics for partitions: " + removedPartitionIds + " in table: "
            + tableNameWithType;
      } else {
        throw new WebApplicationException(
            "TableDataManager is not RealtimeTableDataManager for table: " + tableNameWithType,
            Response.Status.BAD_REQUEST);
      }
    } catch (Exception e) {
      throw new WebApplicationException(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
