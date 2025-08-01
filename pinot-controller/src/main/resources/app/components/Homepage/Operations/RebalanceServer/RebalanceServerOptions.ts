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
export type RebalanceServerOption = {
    name: string;
    label: string;
    type: "BOOL" | "INTEGER" | "SELECT" | "DOUBLE";
    description: string;
    defaultValue: string | boolean | number;
    isAdvancedConfig: boolean;
    isStatsGatheringConfig: boolean;
    markWithWarningIcon: boolean;
    allowedValues?: string[];
    toolTip?: string;
    valueStep?: number;
    valueMin?: number;
    valueMax?: number;
}

export const rebalanceServerOptions: RebalanceServerOption[] = [
    {
        "name": "dryRun",
        "defaultValue": false,
        "label": "Dry Run",
        "type": "BOOL",
        "description": "If enabled, rebalance will not run but expected changes that will occur will be returned",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": true,
        "markWithWarningIcon": false
    },
    {
        "name": "preChecks",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Pre-Checks",
        "description": "If enabled, will perform some pre-checks to ensure rebalance is safe, must enable dryRun to enable this",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": true,
        "markWithWarningIcon": false
    },
    {
        "name": "reassignInstances",
        "defaultValue": true,
        "type": "BOOL",
        "label": "Reassign Instances",
        "description": "If enabled, reassign the instances of the table before making updates to the segment assignment",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": false
    },
    {
        "name": "includeConsuming",
        "defaultValue": true,
        "type": "BOOL",
        "label": "Include Consuming",
        "description": "If enabled, CONSUMING segments will be included in the rebalance of realtime tables. This is mandatory for for upsert/dedup tables",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": false
    },
    {
        "name": "minimizeDataMovement",
        "defaultValue": "ENABLE",
        "type": "SELECT",
        "allowedValues": ["ENABLE", "DISABLE", "DEFAULT"],
        "label": "Minimize Data Movement",
        "description": "If enabled, it reduces the segments that will be moved by trying to minimize the changes to the instance assignment. Setting this to default will fallback to the value of this flag in the TableConfig",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": true,
        "toolTip": "Disabling minimizeDataMovement can cause a large amount of data movement"
    },
    {
        "name": "bootstrap",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Bootstrap",
        "description": "If enabled, regardless of minimum segment movement, reassign all segments in a round-robin fashion as if adding new segments to an empty table",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": true,
        "toolTip": "Enabling bootstrap can cause a large amount of data movement"
    },
    {
        "name": "downtime",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Downtime",
        "description": "If enabled, rebalance will be performed with downtime. This must be set to true if replication = 1",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": true,
        "toolTip": "Enabling can cause downtime, double check if downtime is acceptable if replication > 1"
    },
    {
        "name": "minAvailableReplicas",
        "defaultValue": -1,
        "type": "INTEGER",
        "label": "Min Available Replicas",
        "description": "For no-downtime rebalance, minimum number of replicas to keep alive during rebalance if value is positive, or 'numReplicas + value' replicas to keep alive if value is negative (e.g. if -1, replicas to keep alive = 'numReplicas + (-1)'). Should not be 0 unless for downtime=true",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": false
    },
    {
        "name": "batchSizePerServer",
        "defaultValue": -1,
        "type": "INTEGER",
        "label": "Batch Size Per Server",
        "description": "Batch size of segments to add per server in each rebalance step. For non-strict replica group this serves as the maximum per server, for strict replica group since a partition is moved as a whole, this serves as best efforts. Defaults to -1 to disable batching. Recommendation: Run Dry Run and check how many segments are to be moved per server, if this number is > 200, enable batching by setting this option to about 100 to 200, otherwise leave it at the default",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": false
    },
    {
        "name": "lowDiskMode",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Low Disk Mode",
        "description": "If enabled, perform rebalance by offloading segments off servers prior to adding them. Can slow down rebalance and is recommended to enable for scenarios which are low on disk capacity",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": false
    },
    {
        "name": "bestEfforts",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Best Efforts",
        "description": "If enabled, even if downtime=false do not fail rebalance if IS-EV convergence fails within timeout or segments are in ERROR state and continue rebalancing. This can cause downtime",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": true,
        "toolTip": "Enabling can cause downtime even if downtime=false"
    },
    {
        "name": "externalViewStabilizationTimeoutInMs",
        "defaultValue": 3600000,
        "type": "INTEGER",
        "label": "External View Stabilization Timeout (ms)",
        "description": "Maximum time (in milliseconds) to wait for external view to converge with ideal states. It automatically extends the time if progress has been made",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": false
    },
    {
        "name": "maxAttempts",
        "defaultValue": 3,
        "type": "INTEGER",
        "label": "Max Attempts",
        "description": "Max number of attempts to rebalance",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": false
    },
    {
        "name": "updateTargetTier",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Update Target Tier",
        "description": "If enabled, update segment target tier as part of the rebalance",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": false
    },
    {
        "name": "diskUtilizationThreshold",
        "defaultValue": -1.0,
        "type": "DOUBLE",
        "label": "Disk Utilization Threshold",
        "description": "Override disk utilization threshold used in pre-check (0.0 to 1.0, e.g., 0.85 for 85%). If not provided (or any negative value), uses the controller's default threshold",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": false,
        "valueStep": 0.05,
        "valueMin": -1.0,
        "valueMax": 1.0
    },
    {
        "name": "forceCommit",
        "defaultValue": false,
        "type": "BOOL",
        "label": "Force Commit",
        "description": "Do force commit on consuming segments before they are rebalanced",
        "isAdvancedConfig": false,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": false
    },
    {
        "name": "forceCommitBatchSize",
        "defaultValue": 2147483647,
        "type": "INTEGER",
        "label": "Force Commit Batch Size",
        "description": "If forceCommit is set, this is the batch size for force commit operations. Controls how many segments are force committed in each batch. (Default to Integer.MAX to disable batching)",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": false
    },
    {
        "name": "forceCommitBatchStatusCheckTimeoutMs",
        "defaultValue": 180000,
        "type": "INTEGER",
        "label": "Force Commit Status Check Timeout (ms)",
        "description": "If forceCommit is set, this is the timeout in milliseconds for force commit batch status checks. Maximum time to wait for force commit operations to complete",
        "isAdvancedConfig": true,
        "isStatsGatheringConfig": false,
        "markWithWarningIcon": false
    }
]