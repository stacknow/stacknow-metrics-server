/**
 * File: kubeNodeStatsDetailed_v2.js
 * Description:
 *   - Collects detailed K8s node stats (usage, capacity, allocatable, requests, conditions, etc.).
 *   - !! DELETES ALL PREVIOUS RECORDS and stores ONLY the latest snapshot into MySQL. !!
 *   - Provides API endpoints to query the detailed stats (though history is lost).
 *   - Uses updated @kubernetes/client-node response handling and API call formats.
 * Prerequisites:
 *   - Project's package.json MUST include "type": "module"
 *   - MySQL database 'kube_stats_db' must exist (or whatever METRICSDB_NAME is).
 *   - MySQL table 'kube_node_stats_detailed' must be created.
 *   - Metrics Server must be installed in the cluster for usage metrics.
 *   - If running locally: Valid ~/.kube/config pointing to your cluster.
 *   - If running in-cluster: Service account needs permissions for:
 *     nodes (get, list), pods (list all namespaces), nodes/metrics (custom resource get/list via metrics.k8s.io).
 *
 * WARNING: This version DELETES all data in the table before inserting the new snapshot.
 *          This means ALL HISTORICAL DATA IS LOST on each collection cycle.
 *          Use this only if you explicitly require only the latest state and understand this limitation.
 */

// --- Core Dependencies ---
import express from "express";
import cron from "node-cron";
import dotenv from "dotenv";
dotenv.config();
// import mysql from "mysql2/promise"; // Imported via db.js

// --- Kubernetes Client and utilities ---
import * as k8s from '@kubernetes/client-node'; // Import all for k8s.CoreV1Api etc.

// --- Create Express App ---
const app = express();
app.use(express.json());

// --- Database Connection Pool ---
import { metricsDb } from "./utils/db.js"; // Assuming this exports the metricsDb pool

// --- Kubernetes Client Initialization (from ./utils/k8sClient.js) ---
import {
  initKubernetesClients as initExternalK8sClients, // Renaming to avoid conflict
  coreV1Api as externalCoreV1Api,
  // appsV1Api, // Not used in this script
  // networkingV1Api, // Not used
  // policyV1Api, // Not used
  customObjectsApi as externalCustomObjectsApi, // Assuming k8sClient.js exports this
  kc as externalKc // The KubeConfig instance from k8sClient.js
} from './utils/k8sClient.js';


// --- Global K8s Client Variables for this script ---
let k8sCoreV1Api;
let k8sCustomObjectsApi;
let k8sClientInitialized = false;
// We are using the `initKubernetesClients` from `utils/k8sClient.js`
// The `try...catch` block below seems to be an alternative or redundant initialization.
// I will assume for now that `utils/k8sClient.js` correctly initializes and exports
// `coreV1Api` and `customObjectsApi`.

(async () => {
    try {
        console.log("[INFO] Initializing Kubernetes clients via external k8sClient.js...");
        const initSuccess = await initExternalK8sClients(); // Call the initializer from k8sClient.js

        if (initSuccess && externalCoreV1Api && externalCustomObjectsApi) {
            k8sCoreV1Api = externalCoreV1Api; // Assign to local script variables
            k8sCustomObjectsApi = externalCustomObjectsApi;
            k8sClientInitialized = true;
            const currentContext = externalKc.getCurrentContext();
            const currentCluster = externalKc.getCluster(externalKc.getContextObject(currentContext).cluster);
            console.log(`[INFO] K8s client initialized successfully from k8sClient.js. Context: "${currentContext}", Server: "${currentCluster?.server}"`);
        } else {
            throw new Error("Initialization from k8sClient.js failed or essential APIs are missing.");
        }
    } catch (err) {
        console.error("[FATAL] Failed to initialize Kubernetes client using k8sClient.js:", err.message);
        console.error("[FATAL] Kubernetes related features will be unavailable.");
        // process.exit(1); // Optional: Exit if K8s is critical
    }
})();


// --- Helper Functions ---
function parseCpu(cpuStr) {
    if (cpuStr === null || cpuStr === undefined) return 0;
    const str = String(cpuStr).trim();
    if (!str) return 0;
    try {
        if (str.endsWith("m")) { const num = parseFloat(str.slice(0, -1)); return !isNaN(num) ? num / 1000 : 0; }
        if (str.endsWith("u")) { const num = parseFloat(str.slice(0, -1)); return !isNaN(num) ? num / 1000000 : 0; }
        if (str.endsWith("n")) { const num = parseFloat(str.slice(0, -1)); return !isNaN(num) ? num / 1000000000 : 0; }
        const num = parseFloat(str);
        return !isNaN(num) ? num : 0;
    } catch (e) {
        console.warn(`[WARN] Failed to parse CPU string "${cpuStr}":`, e.message);
        return 0;
    }
}

function parseMemory(memStr) {
    if (memStr === null || memStr === undefined) return 0;
    const str = String(memStr).trim();
    if (!str) return 0;
    try {
        const regex = /^([0-9.]+)([eEinumkKMGTP]?i?)?$/;
        const match = str.match(regex);
        if (!match) { const num = parseFloat(str); return !isNaN(num) ? num : 0; }

        const value = parseFloat(match[1]);
        if (isNaN(value)) return 0;
        const unit = match[2] || '';
        switch (unit) {
            case 'Ki': return value * Math.pow(1024, 1); case 'Mi': return value * Math.pow(1024, 2);
            case 'Gi': return value * Math.pow(1024, 3); case 'Ti': return value * Math.pow(1024, 4);
            case 'Pi': return value * Math.pow(1024, 5); case 'Ei': return value * Math.pow(1024, 6);
            case 'k':  return value * Math.pow(1000, 1); case 'M':  return value * Math.pow(1000, 2);
            case 'G':  return value * Math.pow(1000, 3); case 'T':  return value * Math.pow(1000, 4);
            case 'P':  return value * Math.pow(1000, 5); case 'E':  return value * Math.pow(1000, 6);
            case '': default: return value;
        }
    } catch (e) {
        console.warn(`[WARN] Failed to parse Memory string "${memStr}":`, e.message);
        return 0;
    }
}

function safeJsonStringify(obj, defaultValue = null) {
    try {
        if (obj === null || obj === undefined) {
            return typeof defaultValue === 'string' ? defaultValue : JSON.stringify(defaultValue);
        }
        return JSON.stringify(obj);
    } catch (e) {
        console.warn("[WARN] Failed to stringify object:", e.message, "(Returning default)");
        try {
            return typeof defaultValue === 'string' ? defaultValue : JSON.stringify(defaultValue);
        } catch (e2) {
            console.error("[ERROR] Failed to stringify default value as well:", e2.message);
            return 'null';
        }
    }
}

function calculatePercentage(value, total, precision = 4) {
    const numValue = Number(value);
    const numTotal = Number(total);
    if (isNaN(numValue) || isNaN(numTotal) || numTotal <= 0) {
        return 0.0;
    }
    const percentage = (numValue / numTotal) * 100;
    return parseFloat(Math.max(0, percentage).toFixed(precision));
}


// --- Kubernetes Data Collection Logic ---
async function collectAndStoreNodeDetails() {
    if (!k8sClientInitialized) {
        console.warn("[WARN] Skipping collection: Kubernetes client not initialized or failed.");
        return 0;
    }
    if (!k8sCoreV1Api || !k8sCustomObjectsApi) {
        console.warn("[WARN] Skipping collection: One or more required K8s API clients (k8sCoreV1Api, k8sCustomObjectsApi) are missing from local script scope.");
        return 0;
    }

    const collectionTimestamp = new Date();
    console.log(`[INFO] Starting detailed node stats collection cycle at ${collectionTimestamp.toISOString()}`);

    let nodeMetricsItems = [];
    let nodeSpecItems = [];
    let podItems = [];
    let metricsOk = false;
    let specsOk = false;
    let podsOk = false;

    // --- 1. Get Node Metrics ---
    try {
        const group = "metrics.k8s.io";
        const version = "v1beta1";
        const plural = "nodes";
        console.log(`[DEBUG] K8s API Call: listClusterCustomObject (Nodes - Group: ${group}, Version: ${version}, Plural: ${plural})`);
        // Corrected API call: arguments are passed directly
        const metricsRes = await k8sCustomObjectsApi.listClusterCustomObject(group, version, plural);

        if (metricsRes.body && metricsRes.body.items && Array.isArray(metricsRes.body.items)) {
            nodeMetricsItems = metricsRes.body.items;
            metricsOk = true;
            console.log(`[INFO] K8s Metrics API: Fetched ${nodeMetricsItems.length} node metric items.`);
        } else {
            console.warn("[WARN] K8s Node Metrics API: Response structure unexpected or empty. Expected '.body.items' array. Response:", JSON.stringify(metricsRes.body).substring(0, 500) + '...');
        }
    } catch (err) {
        const statusCode = err.response?.statusCode || 'N/A';
        const errorDetails = err.response?.body?.message || err.message || err;
        const errorString = String(errorDetails).substring(0, 500) + (String(errorDetails).length > 500 ? '...' : '');

        if (String(errorDetails).includes("Invalid URL")) {
             console.error("[ERROR] K8s Node Metrics API failed: Invalid URL. KubeConfig might not have loaded the server address correctly.");
        } else if (statusCode === 404) {
            console.error("[ERROR] K8s Node Metrics API: Endpoint not found (404). Is Kubernetes Metrics Server installed and running?");
        } else if (statusCode === 403) {
            console.error(`[ERROR] K8s Node Metrics API: Permission denied (403). Check RBAC for 'get/list' on 'nodes/metrics' in 'metrics.k8s.io'. Message: ${errorString}`);
        } else {
            console.error(`[ERROR] K8s Node Metrics API failed (Status: ${statusCode}):`, errorString);
        }
        if (err.response?.body) console.error("[ERROR_DETAIL] K8s Node Metrics API Response Body:", JSON.stringify(err.response.body));
    }

    // --- 2. Get Node Specifications ---
    try {
        console.log("[DEBUG] K8s API Call: listNode");
        const nodesRes = await k8sCoreV1Api.listNode();

        if (nodesRes.body && nodesRes.body.items && Array.isArray(nodesRes.body.items)) {
            nodeSpecItems = nodesRes.body.items;
            specsOk = true;
            console.log(`[INFO] K8s Core API: Fetched ${nodeSpecItems.length} node specifications.`);
        } else {
            console.error("[ERROR] K8s Core API: listNode response invalid or empty. Expected '.body.items' array. Response:", JSON.stringify(nodesRes.body).substring(0, 500) + '...');
            return 0; // Cannot proceed meaningfully
        }
    } catch (err) {
        const statusCode = err.response?.statusCode || 'N/A';
        const errorDetails = err.response?.body?.message || err.message || err;
        const errorString = String(errorDetails).substring(0, 500) + (String(errorDetails).length > 500 ? '...' : '');

        if (String(errorDetails).includes("Invalid URL")) {
             console.error("[ERROR] K8s Core API failed fetching nodes: Invalid URL. Check KubeConfig loading.");
        } else if (statusCode === 403) {
             console.error(`[ERROR] K8s Core API: Permission denied (403) listing nodes. Check RBAC for 'list' on 'nodes'. Message: ${errorString}`);
        } else {
             console.error(`[ERROR] K8s Core API failed fetching nodes (Status: ${statusCode}):`, errorString);
        }
        if (err.response?.body) console.error("[ERROR_DETAIL] K8s Core API (listNode) Response Body:", JSON.stringify(err.response.body));
        return 0; // Cannot proceed without node specs
    }

     // --- 3. Get All Pods ---
    try {
        console.log("[DEBUG] K8s API Call: listPodForAllNamespaces");
        const fieldSelector = 'spec.nodeName!=,status.phase!=Succeeded,status.phase!=Failed';
        const podsRes = await k8sCoreV1Api.listPodForAllNamespaces(
            undefined, undefined, undefined, fieldSelector, undefined
        );

        if (podsRes.body && podsRes.body.items && Array.isArray(podsRes.body.items)) {
            podItems = podsRes.body.items;
            podsOk = true;
            console.log(`[INFO] K8s Core API: Fetched ${podItems.length} relevant pods.`);
        } else {
            console.error("[ERROR] K8s Core API: listPodForAllNamespaces response invalid or empty. Expected '.body.items' array. Response:", JSON.stringify(podsRes.body).substring(0, 500) + '...');
        }
    } catch (err) {
        const statusCode = err.response?.statusCode || 'N/A';
        const errorDetails = err.response?.body?.message || err.message || err;
        const errorString = String(errorDetails).substring(0, 500) + (String(errorDetails).length > 500 ? '...' : '');

        if (String(errorDetails).includes("Invalid URL")) {
             console.error("[ERROR] K8s Core API failed fetching pods: Invalid URL. Check KubeConfig loading.");
        } else if (statusCode === 403) {
             console.error(`[ERROR] K8s Core API: Permission denied (403) listing pods for all namespaces. Check RBAC for 'list' on 'pods' cluster-wide. Message: ${errorString}`);
        } else {
             console.error(`[ERROR] K8s Core API failed fetching pods (Status: ${statusCode}):`, errorString);
        }
        if (err.response?.body) console.error("[ERROR_DETAIL] K8s Core API (listPodForAllNamespaces) Response Body:", JSON.stringify(err.response.body));
    }

    // --- 4. Process Fetched Data ---
    const usageMap = new Map();
    if (metricsOk) {
        nodeMetricsItems.forEach(metric => {
            const name = metric.metadata?.name;
            if (!name) { console.warn("[WARN] Found node metric item without a name:", metric.metadata?.uid || "Unknown UID"); return; }
            const cpuUsage = metric.usage?.cpu; const memUsage = metric.usage?.memory;
            let cpuUsed = 0; let memUsed = 0;
            if (cpuUsage && memUsage) {
                cpuUsed = parseCpu(cpuUsage); memUsed = parseMemory(memUsage);
            } else { // metrics.k8s.io/v1beta1 might not have .usage directly for nodes, but for containers on node if aggregated
                console.log(`[DEBUG] No top-level usage block for node ${name}. This might be normal for some Metrics Server versions/configs.`);
            }
            // Some metrics servers might only provide window, not direct usage.
            // For now, we only rely on .usage.cpu and .usage.memory if present.
            usageMap.set(name, { cpuUsedCores: cpuUsed, memUsedBytes: memUsed });
        });
    } else { console.warn("[WARN] Skipping usage processing as node metrics fetch failed or returned no items."); }

    const podDataMap = new Map();
    if (podsOk) {
        podItems.forEach(pod => {
            const nodeName = pod.spec?.nodeName; if (!nodeName) return;
            if (!podDataMap.has(nodeName)) { podDataMap.set(nodeName, { count: 0, requestedCpu: 0, requestedMemory: 0 }); }
            const nodeData = podDataMap.get(nodeName); nodeData.count++;
            const sumContainerRequests = (containers) => {
                if (!Array.isArray(containers)) return;
                containers.forEach(container => {
                    if (container.resources?.requests) {
                        nodeData.requestedCpu += parseCpu(container.resources.requests.cpu);
                        nodeData.requestedMemory += parseMemory(container.resources.requests.memory);
                    }
                });
            };
            sumContainerRequests(pod.spec?.containers);
            sumContainerRequests(pod.spec?.initContainers);
            if (pod.spec?.overhead) {
                 nodeData.requestedCpu += parseCpu(pod.spec.overhead.cpu);
                 nodeData.requestedMemory += parseMemory(pod.spec.overhead.memory);
            }
        });
    } else { console.warn("[WARN] Skipping pod request/count processing as pod fetch failed or returned no items."); }

    // --- 5. Combine Data and Prepare Records ---
    const recordsToInsert = [];
    for (const node of nodeSpecItems) { // Iterate through nodes (specsOk ensures 'nodeSpecItems' is valid)
        const nodeName = node.metadata?.name;
        if (!nodeName) { console.warn("[WARN] Skipping node specification with missing metadata.name:", node.metadata?.uid || "Unknown UID"); continue; }

        const usage = metricsOk ? (usageMap.get(nodeName) || { cpuUsedCores: 0, memUsedBytes: 0 }) : { cpuUsedCores: 0, memUsedBytes: 0 };
        const podData = podsOk ? (podDataMap.get(nodeName) || { count: 0, requestedCpu: 0, requestedMemory: 0 }) : { count: 0, requestedCpu: 0, requestedMemory: 0 };

        const status = node.status || {}; const spec = node.spec || {};
        const nodeInfo = status.nodeInfo || {}; const labels = node.metadata?.labels || {};

        const capacityCpu = parseCpu(status.capacity?.cpu); const capacityMem = parseMemory(status.capacity?.memory);
        const capacityPodsRaw = parseInt(status.capacity?.pods || '0', 10); const capacityPods = isNaN(capacityPodsRaw) ? 0 : capacityPodsRaw;
        const allocatableCpu = parseCpu(status.allocatable?.cpu); const allocatableMem = parseMemory(status.allocatable?.memory);
        const allocatablePodsRaw = parseInt(status.allocatable?.pods || '0', 10); const allocatablePods = isNaN(allocatablePodsRaw) ? 0 : allocatablePodsRaw;

        let isReady = false; const conditions = status.conditions || [];
        const readyCondition = conditions.find(c => c.type === 'Ready');
        if (readyCondition && readyCondition.status === 'True') { isReady = true; }

        const instanceType = labels['node.kubernetes.io/instance-type'] || labels['beta.kubernetes.io/instance-type'] || null;
        const nodePoolName = labels['eks.amazonaws.com/nodegroup'] || labels['cloud.google.com/gke-nodepool'] || labels['agentpool'] || labels['kubernetes.azure.com/agentpool'] || labels['node.kubernetes.io/instancegroup'] || labels['pool'] || null;
        const nodeZone = labels['topology.kubernetes.io/zone'] || labels['failure-domain.beta.kubernetes.io/zone'] || null;
        const nodeRegion = labels['topology.kubernetes.io/region'] || labels['failure-domain.beta.kubernetes.io/region'] || null;

        const record = {
            node_name: nodeName, collected_at: collectionTimestamp, instance_type: instanceType,
            node_pool: nodePoolName, zone: nodeZone, region: nodeRegion,
            architecture: nodeInfo.architecture || null, operating_system: nodeInfo.operatingSystem || null,
            os_image: nodeInfo.osImage || null, kernel_version: nodeInfo.kernelVersion || null,
            kubelet_version: nodeInfo.kubeletVersion || null,
            labels: safeJsonStringify(labels, '{}'), taints: safeJsonStringify(spec.taints, '[]'),
            is_ready: isReady, conditions: safeJsonStringify(conditions, '[]'),
            capacity_cpu_cores: capacityCpu, capacity_memory_bytes: capacityMem, capacity_pods: capacityPods,
            allocatable_cpu_cores: allocatableCpu, allocatable_memory_bytes: allocatableMem, allocatable_pods: allocatablePods,
            usage_cpu_cores: usage.cpuUsedCores, usage_memory_bytes: usage.memUsedBytes,
            requested_cpu_cores: podData.requestedCpu, requested_memory_bytes: podData.requestedMemory,
            pod_count_current: podData.count,
            utilization_cpu_vs_allocatable: calculatePercentage(usage.cpuUsedCores, allocatableCpu),
            utilization_memory_vs_allocatable: calculatePercentage(usage.memUsedBytes, allocatableMem),
            utilization_pods_vs_allocatable: calculatePercentage(podData.count, allocatablePods),
            pressure_cpu_requests_vs_allocatable: calculatePercentage(podData.requestedCpu, allocatableCpu),
            pressure_memory_requests_vs_allocatable: calculatePercentage(podData.requestedMemory, allocatableMem),
        };
        recordsToInsert.push(record);
    }

    // --- 6. Insert Records into Database (DELETING ALL PREVIOUS RECORDS FIRST) ---
    if (!specsOk) {
         console.warn("[WARN] Skipping database update because initial node specification fetch failed. Table state remains unchanged.");
         return 0;
    }
    if (recordsToInsert.length === 0 && nodeSpecItems.length > 0) {
         console.log("[INFO] Node specs fetched, but processing resulted in zero records. Proceeding to clear existing data.");
    } else if (recordsToInsert.length === 0 && nodeSpecItems.length === 0) {
         console.log("[INFO] Node specs fetched successfully, and 0 nodes found in API. Proceeding to clear existing data.");
    }

    // Use metricsDb directly
    let connection = null;
    const insertStmt = `
        INSERT INTO kube_node_stats_detailed (
            node_name, collected_at, instance_type, node_pool, zone, region, architecture, operating_system,
            os_image, kernel_version, kubelet_version, labels, taints, is_ready,
            conditions, capacity_cpu_cores, capacity_memory_bytes, capacity_pods,
            allocatable_cpu_cores, allocatable_memory_bytes, allocatable_pods,
            usage_cpu_cores, usage_memory_bytes, requested_cpu_cores, requested_memory_bytes,
            pod_count_current, utilization_cpu_vs_allocatable, utilization_memory_vs_allocatable,
            utilization_pods_vs_allocatable, pressure_cpu_requests_vs_allocatable,
            pressure_memory_requests_vs_allocatable
        ) VALUES ?`;

    const values = recordsToInsert.map(r => [
        r.node_name, r.collected_at, r.instance_type, r.node_pool, r.zone, r.region, r.architecture, r.operating_system,
        r.os_image, r.kernel_version, r.kubelet_version, r.labels, r.taints, r.is_ready,
        r.conditions, r.capacity_cpu_cores, r.capacity_memory_bytes, r.capacity_pods,
        r.allocatable_cpu_cores, r.allocatable_memory_bytes, r.allocatable_pods,
        r.usage_cpu_cores, r.usage_memory_bytes, r.requested_cpu_cores, r.requested_memory_bytes,
        r.pod_count_current, r.utilization_cpu_vs_allocatable, r.utilization_memory_vs_allocatable,
        r.utilization_pods_vs_allocatable, r.pressure_cpu_requests_vs_allocatable,
        r.pressure_memory_requests_vs_allocatable
    ]);

    try {
        connection = await metricsDb.getConnection(); // Get connection from the pool
        await connection.beginTransaction();
        console.log("[INFO] Database Transaction Started.");

        console.log("[INFO] Deleting all existing records from kube_node_stats_detailed...");
        const [deleteResult] = await connection.query('DELETE FROM kube_node_stats_detailed');
        console.log(`[INFO] Deleted ${deleteResult.affectedRows} old records.`);

        let insertedRows = 0;
        if (recordsToInsert.length > 0) {
             console.log(`[INFO] Inserting ${recordsToInsert.length} new node detail records...`);
            const [insertResult] = await connection.query(insertStmt, [values]);
            insertedRows = insertResult.affectedRows;
            console.log(`[INFO] Successfully inserted ${insertedRows} new node detail records.`);
        } else {
            console.log("[INFO] No new records to insert. Table remains empty after delete.");
        }

        await connection.commit();
        console.log("[INFO] Database Transaction Committed.");
        return insertedRows;

    } catch (err) {
        console.error("[ERROR] Database transaction failed:", err.message, err.sqlMessage ? `(SQLState: ${err.sqlState}, SQLMessage: ${err.sqlMessage})` : '');
        if (connection) {
            try { await connection.rollback(); console.log("[INFO] DB transaction rolled back."); }
            catch (rbErr) { console.error("[ERROR] DB rollback failed:", rbErr.message); }
        }
        if (err.sqlMessage && recordsToInsert.length > 0 && values.length > 0) { // Check values also
            console.error("[DEBUG] Sample record (values) potentially involved in failed insert:", JSON.stringify(values[0]).substring(0, 500) + '...');
        }
        return 0;
    } finally {
        if (connection) {
            connection.release();
            console.log("[DEBUG] Database connection released."); // Changed to DEBUG
        }
    }
}


// --- Cron Scheduling ---
const collectionIntervalMinutes = process.env.NODE_STATS_DETAILED_INTERVAL_MINUTES || 1;
const collectionCronExpr = `*/${collectionIntervalMinutes} * * * *`;

console.log(`[INFO] Detailed Node Stats Collection Interval: ${collectionIntervalMinutes} minute(s)`);
let isCollecting = false;

if (!cron.validate(collectionCronExpr)) {
    console.error(`[FATAL] Invalid cron expression: "${collectionCronExpr}". Collector disabled.`);
} else {
    cron.schedule(collectionCronExpr, async () => {
        if (!k8sClientInitialized) {
            console.warn(`[WARN] Cron: Skipping node stats collection, K8s client not ready.`);
            return;
        }
        if (isCollecting) { console.warn(`[WARN] Collection cycle skipped: Previous cycle still running.`); return; }
        isCollecting = true;
        const startTime = Date.now();
        console.log(`[INFO] Cron triggered: Running detailed node collection...`);
        try {
            const insertedCount = await collectAndStoreNodeDetails();
            const duration = (Date.now() - startTime) / 1000;
            console.log(`[INFO] Cron finished: Collection ended (${insertedCount} records inserted, ${duration.toFixed(2)}s).`);
        } catch (err) {
            console.error("[ERROR] Uncaught error in scheduled collection task:", err.message, err.stack);
             const duration = (Date.now() - startTime) / 1000;
             console.log(`[INFO] Cron finished: Collection ended with error (${duration.toFixed(2)}s).`);
        } finally {
            isCollecting = false;
        }
    }, { scheduled: true, timezone: "Etc/UTC" }); // Ensure timezone is set for cron
    console.log(`[INFO] Collection cron job scheduled with expression: ${collectionCronExpr}`);
}


// --- API Endpoints ---
app.get("/healthz", (req, res) => {
    res.status(200).json({ status: "ok", timestamp: new Date().toISOString() });
});

app.get("/api/v1/nodes/latest", async (req, res) => {
    // Use metricsDb directly
    try {
        const [rows] = await metricsDb.query(`
            SELECT *
            FROM kube_node_stats_detailed
            ORDER BY node_name ASC
        `);
        if (!rows || rows.length === 0) {
             return res.status(404).json({ error: "No node data found in the current snapshot." });
        }
        res.json({
            data: rows,
            count: rows.length,
            snapshot_timestamp: rows.length > 0 ? rows[0].collected_at : null
        });
    } catch (err) {
        console.error("[API ERROR] Failed to fetch latest node stats:", err);
        res.status(500).json({ error: "Internal Server Error", details: err.message });
    }
});

app.get("/api/v1/nodes/latest/:nodeName", async (req, res) => {
    const { nodeName } = req.params;
    if (!nodeName) {
        return res.status(400).json({ error: "Missing nodeName parameter" });
    }
    // Use metricsDb directly
    try {
        const [rows] = await metricsDb.query(
            'SELECT * FROM kube_node_stats_detailed WHERE node_name = ?',
            [nodeName]
        );
        if (!rows || rows.length === 0) {
            return res.status(404).json({ error: `No data found for node '${nodeName}' in the current snapshot.` });
        }
        res.json({
            data: rows[0],
            snapshot_timestamp: rows[0].collected_at
        });
    } catch (err) {
        console.error(`[API ERROR] Failed to fetch latest stats for node ${nodeName}:`, err);
        res.status(500).json({ error: "Internal Server Error", details: err.message });
    }
});

// --- Graceful Shutdown ---
async function gracefulShutdown(signal) {
    console.log(`\n[INFO] Received signal: ${signal}. Shutting down gracefully...`);
    console.log("[INFO] Stopping cron tasks...");
    cron.getTasks().forEach(task => task.stop());
    isCollecting = true;
    console.log("[INFO] Waiting briefly (e.g., 2s) for ongoing tasks...");
    await new Promise(resolve => setTimeout(resolve, 2000));
    console.log("[INFO] Closing database connection pool (metricsDb)...");
    try {
        await metricsDb.end(); // Use metricsDb directly
        console.log("[INFO] Database pool closed.");
    }
    catch (dbErr) { console.error("[ERROR] Error closing database pool:", dbErr.message); }
    console.log("[INFO] Shutdown complete. Exiting.");
    process.exit(0);
}
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));


// --- Start Express Server ---
const PORT = process.env.NODE_STATS_DETAILED_PORT || process.env.PORT || 5201;
const HOST = "0.0.0.0";

app.listen(PORT, HOST, () => {
  console.log(`[INFO] K8s Detailed Node Stats Server running at http://${HOST}:${PORT}`);
  console.warn("[WARNING] This server version DELETES historical node data on each collection cycle.");

  if (!k8sClientInitialized) {
    console.error("[FATAL ERROR] Server started, BUT Kubernetes client initialization failed earlier. Data collection WILL NOT WORK.");
  } else {
    console.log("[INFO] Server ready.");
    const scheduledTasks = cron.getTasks();
    if (scheduledTasks.length > 0 && k8sClientInitialized) { // Ensure client is also ready for cron to be meaningful
      console.log("[INFO] Cron job scheduled.");
    } else if (k8sClientInitialized) { // Client is ready, but cron didn't schedule (e.g. invalid expr)
      console.warn("[WARN] Cron job was NOT scheduled (check validation logs).");
    }
    // If !k8sClientInitialized, the earlier FATAL ERROR message covers it.
  }
});