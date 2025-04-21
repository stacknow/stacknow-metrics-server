/**
 * File: kubeNodeStatsDetailed_v2.js
 * Description:
 *   - Collects detailed K8s node stats (usage, capacity, allocatable, requests, conditions, etc.).
 *   - Stores raw stats directly into MySQL for later analysis/aggregation.
 *   - Provides API endpoints to query the detailed stats.
 *   - Uses updated @kubernetes/client-node response handling and API call formats.
 * Prerequisites:
 *   - Project's package.json MUST include "type": "module"
 *   - MySQL database 'kube_stats_db' must exist.
 *   - MySQL table 'kube_node_stats_detailed' must be created (schema provided previously).
 *   - Metrics Server must be installed in the cluster for usage metrics.
 *   - If running locally: Valid ~/.kube/config pointing to your cluster.
 *   - If running in-cluster: Service account needs permissions for:
 *     nodes (get, list), pods (list all namespaces), nodes/metrics (custom resource get/list via metrics.k8s.io).
 */

// --- Core Dependencies ---
import express from "express";
import cron from "node-cron";
import dotenv from "dotenv";
dotenv.config();
import mysql from "mysql2/promise";

// --- Kubernetes Client ---
import * as k8s from "@kubernetes/client-node";

// --- Create Express App ---
const app = express();
app.use(express.json());

// --- Database Connection Pool ---
const pool = mysql.createPool({
    host: process.env.MYSQL_HOST || "database-1.cxyc4mq4ohwl.us-east-1.rds.amazonaws.com",
    port: process.env.MYSQL_PORT ? parseInt(process.env.MYSQL_PORT, 10) : 3306,
    user: process.env.MYSQL_USER || "stacknow_user",
    password: process.env.MYSQL_PASSWORD || "stacknow_password", // Set your password in .env
    database: process.env.MYSQL_DATABASE || "kube_stats_db",        // Ensure this database exists
    waitForConnections: true,
    connectionLimit: 15, // Increased slightly for potentially concurrent API calls + DB operations
    queueLimit: 0,
    decimalNumbers: true, // Ensure numbers like CPU/Memory aren't strings
    dateStrings: false,   // Get Date objects, not strings
    flags: ["+FOUND_ROWS"]
});
const getDB = () => pool;

// --- Kubernetes Client Initialization ---
const kc = new k8s.KubeConfig();
let k8sCoreV1Api;
let k8sCustomObjectsApi;
let k8sClientInitialized = false;

try {
    console.log("[INFO] Attempting to load K8s config...");

    // --- CHOOSE ONE based on where you run the script ---
    // 1. For LOCAL development/testing (uses ~/.kube/config or KUBECONFIG env var)
    kc.loadFromDefault();
    console.log("[INFO] K8s config loaded using loadFromDefault(). Ensure ~/.kube/config is valid.");

    // 2. For running INSIDE a Kubernetes POD (uses service account)
    // kc.loadFromCluster();
    // console.log("[INFO] K8s config loaded using loadFromCluster(). Ensure Service Account has permissions.");
    // --- End of choice ---

    k8sCoreV1Api = kc.makeApiClient(k8s.CoreV1Api);
    k8sCustomObjectsApi = kc.makeApiClient(k8s.CustomObjectsApi);

    // Validate that clients were created and config has a server address
    if (!k8sCoreV1Api || !k8sCustomObjectsApi) {
        throw new Error("Failed to create one or more Kubernetes API clients after loading config.");
    }

    const currentContext = kc.getCurrentContext();
    if (!currentContext) {
         throw new Error("KubeConfig loaded, but no current context is set.");
    }
    const currentContextObject = kc.getContextObject(currentContext);
     if (!currentContextObject || !currentContextObject.cluster) {
        throw new Error(`KubeConfig loaded, but context "${currentContext}" has no associated cluster.`);
    }
    const currentCluster = kc.getCluster(currentContextObject.cluster);
    if (!currentCluster || !currentCluster.server) {
        throw new Error(`KubeConfig loaded, but the cluster for context "${currentContext}" has no server address defined.`);
    }
    console.log(`[INFO] K8s client configured for context "${currentContext}" targeting server "${currentCluster.server}"`);

    k8sClientInitialized = true;
    console.log("[INFO] Kubernetes client initialization seems successful.");

} catch (err) {
    console.error("[FATAL] Failed to initialize Kubernetes client:", err.message);
    if (err.code === 'ENOENT' && err.path?.includes('.kube/config')) {
         console.error("[FATAL] Kubeconfig file not found (tried loadFromDefault?). Ensure ~/.kube/config exists or KUBECONFIG env var is set.");
    } else if (err.code === 'ENOENT' && err.path?.includes('serviceaccount/token')) {
         console.error("[FATAL] Service account token not found (tried loadFromCluster?). Ensure this runs inside a pod or use loadFromDefault().");
    } else if (err.message?.includes('permission denied')) {
         console.error("[FATAL] Permission denied reading config/token. Check file permissions or RBAC.");
    } else if (err.message?.includes("no current context is set")) {
         console.error("[FATAL] Your kubeconfig is valid but lacks a 'current-context'. Set one using `kubectl config use-context <context-name>`.");
    } else if (err.message?.includes("no server address defined")) {
         console.error("[FATAL] Your kubeconfig context points to a cluster definition that is missing the 'server:' address.");
    } else {
        console.error("[FATAL] Full initialization error details:", err); // Log the full error for unknown issues
    }
    console.error("[FATAL] Kubernetes related features will be unavailable.");
    // Optional: Exit if K8s is critical
    // process.exit(1);
}


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
        // If regex doesn't match but it's potentially a plain number (bytes)
        if (!match) { const num = parseFloat(str); return !isNaN(num) ? num : 0; }

        const value = parseFloat(match[1]);
        if (isNaN(value)) return 0;
        const unit = match[2] || ''; // Default to bytes if no unit
        switch (unit) {
            case 'Ki': return value * Math.pow(1024, 1); case 'Mi': return value * Math.pow(1024, 2);
            case 'Gi': return value * Math.pow(1024, 3); case 'Ti': return value * Math.pow(1024, 4);
            case 'Pi': return value * Math.pow(1024, 5); case 'Ei': return value * Math.pow(1024, 6);
            case 'k':  return value * Math.pow(1000, 1); case 'M':  return value * Math.pow(1000, 2);
            case 'G':  return value * Math.pow(1000, 3); case 'T':  return value * Math.pow(1000, 4);
            case 'P':  return value * Math.pow(1000, 5); case 'E':  return value * Math.pow(1000, 6);
            case '': default: return value; // Assume bytes if no unit or unknown unit
        }
    } catch (e) {
        console.warn(`[WARN] Failed to parse Memory string "${memStr}":`, e.message);
        return 0;
    }
}

function safeJsonStringify(obj, defaultValue = null) {
    try {
        if (obj === null || obj === undefined) {
            return defaultValue;
        }
        return JSON.stringify(obj);
    } catch (e) {
        console.warn("[WARN] Failed to stringify object:", e.message, "(Returning default)");
        try {
            return typeof defaultValue === 'string' ? defaultValue : JSON.stringify(defaultValue);
        } catch (e2) {
            console.error("[ERROR] Failed to stringify default value as well:", e2.message);
            return 'null'; // Ultimate fallback
        }
    }
}

function calculatePercentage(value, total, precision = 4) {
    const numValue = Number(value);
    const numTotal = Number(total);
    if (isNaN(numValue) || isNaN(numTotal) || numTotal <= 0) { // Check total > 0
        return 0.0;
    }
    const percentage = (numValue / numTotal) * 100;
    return parseFloat(Math.max(0, percentage).toFixed(precision)); // Ensure positive and format
}


// --- Kubernetes Data Collection Logic (Corrected API Calls & Error Handling) ---
async function collectAndStoreNodeDetails() {
    if (!k8sClientInitialized) { // Check if initialization succeeded
        console.warn("[WARN] Skipping collection: Kubernetes client not initialized or failed.");
        return 0;
    }
    // Further check: Ensure specific API clients are available
    if (!k8sCoreV1Api || !k8sCustomObjectsApi) {
        console.warn("[WARN] Skipping collection: One or more required K8s API clients are missing.");
        return 0;
    }

    const collectionTimestamp = new Date();
    console.log(`[INFO] Starting detailed node stats collection cycle at ${collectionTimestamp.toISOString()}`);

    let nodeMetrics = [];
    let nodes = [];
    let pods = [];
    let metricsOk = false;
    let specsOk = false;
    let podsOk = false;

    // --- 1. Get Node Metrics ---
    try {
        const group = "metrics.k8s.io";
        const version = "v1beta1";
        const plural = "nodes";
        console.log(`[DEBUG] K8s API Call: listClusterCustomObject (Nodes - Group: ${group}, Version: ${version}, Plural: ${plural})`);

        // --- API CALL using OBJECT format ---
        const metricsRes = await k8sCustomObjectsApi.listClusterCustomObject({ group, version, plural });

        // --- RESPONSE HANDLING: Access 'items' directly ---
        if (metricsRes && metricsRes.items && Array.isArray(metricsRes.items)) {
            nodeMetrics = metricsRes.items;
            metricsOk = true;
            console.log(`[INFO] K8s Metrics API: Fetched ${nodeMetrics.length} node metric items.`);
        } else {
            console.warn("[WARN] K8s Node Metrics API: Response structure unexpected or empty. Expected '.items' array. Response:", JSON.stringify(metricsRes).substring(0, 500) + '...');
        }
    } catch (err) {
        const statusCode = err.statusCode || err.response?.statusCode || 'N/A';
        // Prioritize K8s API error body, then message
        const errorDetails = err.response?.body || err.message || err;
        const errorString = JSON.stringify(errorDetails).substring(0, 500) + (JSON.stringify(errorDetails).length > 500 ? '...' : '');

        if (errorString.includes("Invalid URL")) { // Check for this specifically
             console.error("[ERROR] K8s Node Metrics API failed: Invalid URL. This likely means the KubeConfig failed to load the server address correctly.");
        } else if (statusCode === 404) {
            console.error("[ERROR] K8s Node Metrics API: Endpoint not found (404). Is Kubernetes Metrics Server installed and running?");
        } else if (statusCode === 403) {
            console.error(`[ERROR] K8s Node Metrics API: Permission denied (403). Check Service Account RBAC for 'get/list' on 'nodes/metrics' in 'metrics.k8s.io'.`);
        } else {
            console.error(`[ERROR] K8s Node Metrics API failed (Status: ${statusCode}):`, errorString);
        }
    }

    // --- 2. Get Node Specifications ---
    try {
        console.log("[DEBUG] K8s API Call: listNode");
        // --- API CALL using NO arguments for basic list ---
        const nodesRes = await k8sCoreV1Api.listNode();

        // --- RESPONSE HANDLING: Access 'items' directly ---
        if (nodesRes && nodesRes.items && Array.isArray(nodesRes.items)) {
            nodes = nodesRes.items;
            specsOk = true;
            console.log(`[INFO] K8s Core API: Fetched ${nodes.length} node specifications.`);
        } else {
            console.error("[ERROR] K8s Core API: listNode response invalid or empty. Expected '.items' array. Response:", JSON.stringify(nodesRes).substring(0, 500) + '...');
            // If node specs fail, we can't proceed meaningfully
            return 0;
        }
    } catch (err) {
        const statusCode = err.statusCode || err.response?.statusCode || 'N/A';
        const errorDetails = err.response?.body || err.message || err;
        const errorString = JSON.stringify(errorDetails).substring(0, 500) + (JSON.stringify(errorDetails).length > 500 ? '...' : '');

        if (errorString.includes("Invalid URL")) {
             console.error("[ERROR] K8s Core API failed fetching nodes: Invalid URL. Check KubeConfig loading.");
        } else if (statusCode === 403) {
             console.error(`[ERROR] K8s Core API: Permission denied (403) listing nodes. Check RBAC for 'list' on 'nodes'.`);
        } else {
             console.error(`[ERROR] K8s Core API failed fetching nodes (Status: ${statusCode}):`, errorString);
        }
        // Cannot proceed without node specs
        return 0;
    }

     // --- 3. Get All Pods (for calculating resource requests) ---
    try {
        console.log("[DEBUG] K8s API Call: listPodForAllNamespaces");
        const fieldSelector = 'spec.nodeName!=,status.phase!=Succeeded,status.phase!=Failed';

        // --- API CALL using POSITIONAL arguments for filters ---
        const podsRes = await k8sCoreV1Api.listPodForAllNamespaces(
            undefined, // pretty
            undefined, // allowWatchBookmarks
            undefined, // continue
            fieldSelector, // fieldSelector (4th positional argument)
            undefined // labelSelector
        );

        // --- RESPONSE HANDLING: Access 'items' directly ---
        if (podsRes && podsRes.items && Array.isArray(podsRes.items)) {
            pods = podsRes.items;
            podsOk = true;
            console.log(`[INFO] K8s Core API: Fetched ${pods.length} relevant pods.`);
        } else {
            console.error("[ERROR] K8s Core API: listPodForAllNamespaces response invalid or empty. Expected '.items' array. Response:", JSON.stringify(podsRes).substring(0, 500) + '...');
        }
    } catch (err) {
        const statusCode = err.statusCode || err.response?.statusCode || 'N/A';
        const errorDetails = err.response?.body || err.message || err;
        const errorString = JSON.stringify(errorDetails).substring(0, 500) + (JSON.stringify(errorDetails).length > 500 ? '...' : '');

        if (errorString.includes("Invalid URL")) {
             console.error("[ERROR] K8s Core API failed fetching pods: Invalid URL. Check KubeConfig loading.");
        } else if (statusCode === 403) {
             console.error(`[ERROR] K8s Core API: Permission denied (403) listing pods for all namespaces. Check RBAC for 'list' on 'pods' cluster-wide.`);
        } else {
             console.error(`[ERROR] K8s Core API failed fetching pods (Status: ${statusCode}):`, errorString);
        }
    }

    // --- 4. Process Fetched Data ---
    const usageMap = new Map();
    if (metricsOk) {
        nodeMetrics.forEach(metric => {
            const name = metric.metadata?.name;
            if (!name) { console.warn("[WARN] Found node metric item without a name:", metric.metadata?.uid || "Unknown UID"); return; }
            const cpuUsage = metric.usage?.cpu; const memUsage = metric.usage?.memory;
            let cpuUsed = 0; let memUsed = 0;
            if (cpuUsage && memUsage) { cpuUsed = parseCpu(cpuUsage); memUsed = parseMemory(memUsage); }
            else if (metric.containers && Array.isArray(metric.containers)) {
                 let containerCpuSum = 0; let containerMemSum = 0;
                 metric.containers.forEach(c => { containerCpuSum += parseCpu(c.usage?.cpu); containerMemSum += parseMemory(c.usage?.memory); });
                 if (containerCpuSum > 0 || containerMemSum > 0) { cpuUsed = containerCpuSum; memUsed = containerMemSum; }
                 else { console.log(`[DEBUG] No usable usage info (top-level or container) found for node ${name} in metrics response.`); }
            } else { console.log(`[DEBUG] No usage block or containers found for node ${name} in metrics response.`); }
            usageMap.set(name, { cpuUsedCores: cpuUsed, memUsedBytes: memUsed });
        });
    } else { console.warn("[WARN] Skipping usage processing as node metrics fetch failed or returned no items."); }

    const podDataMap = new Map();
    if (podsOk) {
        pods.forEach(pod => {
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

    // --- 5. Combine Data and Prepare Records for Database ---
    const recordsToInsert = [];
    // Iterate through nodes (specsOk ensures 'nodes' is valid)
    for (const node of nodes) {
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

        const record = {
            node_name: nodeName, collected_at: collectionTimestamp, instance_type: instanceType,
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

    // --- 6. Insert Records into Database ---
    if (recordsToInsert.length === 0) {
        if (specsOk && nodes.length === 0) { console.log("[INFO] No node records generated: K8s API reported 0 nodes."); }
        else if (specsOk) { console.log("[INFO] No node records generated: Nodes were found but processing resulted in zero records (check warnings/errors above)."); }
        else { console.log("[INFO] No node records generated as node specification fetch failed earlier."); }
        return 0;
    }

    const dbPool = getDB();
    let connection = null;
    // Ensure column names EXACTLY match your MySQL table schema
    const insertStmt = `
        INSERT INTO kube_node_stats_detailed (
            node_name, collected_at, instance_type, architecture, operating_system,
            os_image, kernel_version, kubelet_version, labels, taints, is_ready,
            conditions, capacity_cpu_cores, capacity_memory_bytes, capacity_pods,
            allocatable_cpu_cores, allocatable_memory_bytes, allocatable_pods,
            usage_cpu_cores, usage_memory_bytes, requested_cpu_cores, requested_memory_bytes,
            pod_count_current, utilization_cpu_vs_allocatable, utilization_memory_vs_allocatable,
            utilization_pods_vs_allocatable, pressure_cpu_requests_vs_allocatable,
            pressure_memory_requests_vs_allocatable
        ) VALUES ?`;

    const values = recordsToInsert.map(r => [
        r.node_name, r.collected_at, r.instance_type, r.architecture, r.operating_system,
        r.os_image, r.kernel_version, r.kubelet_version, r.labels, r.taints, r.is_ready,
        r.conditions, r.capacity_cpu_cores, r.capacity_memory_bytes, r.capacity_pods,
        r.allocatable_cpu_cores, r.allocatable_memory_bytes, r.allocatable_pods,
        r.usage_cpu_cores, r.usage_memory_bytes, r.requested_cpu_cores, r.requested_memory_bytes,
        r.pod_count_current, r.utilization_cpu_vs_allocatable, r.utilization_memory_vs_allocatable,
        r.utilization_pods_vs_allocatable, r.pressure_cpu_requests_vs_allocatable,
        r.pressure_memory_requests_vs_allocatable
    ]);

    try {
        connection = await dbPool.getConnection();
        await connection.beginTransaction();
        const [result] = await connection.query(insertStmt, [values]); // Bulk insert
        await connection.commit();
        console.log(`[INFO] Successfully inserted ${result.affectedRows} node detail records.`);
        return result.affectedRows;
    } catch (err) {
        console.error("[ERROR] Database insert failed:", err.message, err.sqlMessage ? `(SQLState: ${err.sqlState}, SQLMessage: ${err.sqlMessage})` : '');
        if (connection) { try { await connection.rollback(); console.log("[INFO] Rolling back DB transaction."); } catch (rbErr) { console.error("[ERROR] DB rollback failed:", rbErr.message); }}
        if (recordsToInsert.length > 0) console.error("[DEBUG] Sample record causing potential issue:", JSON.stringify(recordsToInsert[0]).substring(0, 500) + '...');
        return 0;
    } finally {
        if (connection) { connection.release(); }
    }
}


// --- Cron Scheduling ---
const collectionIntervalMinutes = 1; // Collect frequently (adjust as needed)
const collectionCronExpr = `*/${collectionIntervalMinutes} * * * *`;

console.log(`[INFO] Detailed Node Stats Collection Interval: ${collectionIntervalMinutes} minute(s)`);
let isCollecting = false; // Simple lock

if (!cron.validate(collectionCronExpr)) {
    console.error(`[FATAL] Invalid cron expression: "${collectionCronExpr}". Collector disabled.`);
} else if (!k8sClientInitialized) {
    console.warn("[WARN] K8s client not initialized. Cron job for data collection WILL NOT RUN, but server will start.");
} else {
    cron.schedule(collectionCronExpr, async () => {
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
    }, { scheduled: true, timezone: "Etc/UTC" });
    console.log(`[INFO] Collection cron job scheduled with expression: ${collectionCronExpr}`);
}


// --- Graceful Shutdown ---
async function gracefulShutdown(signal) {
    console.log(`\n[INFO] Received signal: ${signal}. Shutting down gracefully...`);
    console.log("[INFO] Stopping cron tasks...");
    cron.getTasks().forEach(task => task.stop());
    isCollecting = true; // Prevent new collections
    console.log("[INFO] Waiting briefly (e.g., 2s) for ongoing tasks...");
    await new Promise(resolve => setTimeout(resolve, 2000));
    console.log("[INFO] Closing database connection pool...");
    try { await getDB().end(); console.log("[INFO] Database pool closed."); }
    catch (dbErr) { console.error("[ERROR] Error closing database pool:", dbErr.message); }
    console.log("[INFO] Shutdown complete. Exiting.");
    process.exit(0);
}
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));


// --- Start Express Server ---
const PORT = process.env.PORT || 5201;
const HOST = "0.0.0.0"; // Listen on all available interfaces

app.listen(PORT, HOST, () => {
  console.log(`[INFO] K8s Detailed Node Stats Server running at http://${HOST}:${PORT}`);

  if (!k8sClientInitialized) {
    console.error("[FATAL ERROR] Server started, BUT Kubernetes client initialization failed earlier. Data collection WILL NOT WORK.");
  } else {
    console.log("[INFO] Server ready.");
    if (cron.getTasks().length > 0) {
      console.log("[INFO] Cron job scheduled (if validation passed).");
    } else {
      console.warn("[WARN] Cron job was NOT scheduled (check validation/init logs).");
    }
  }
});
