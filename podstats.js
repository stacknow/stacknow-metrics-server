/**
 * File: kubePodStats.js
 * Description:
 *   - Collects K8s pod stats (metrics & specs) using @kubernetes/client-node.
 *   - Buffers, aggregates, and stores stats in MySQL.
 *   - Provides API endpoints for aggregated stats and allocated resources.
 * Prerequisite: Project's package.json MUST include "type": "module"
 * Prerequisite: All imported local utility files must also use ES Module syntax (export).
 */

// --- Core Dependencies ---
import express from "express";
import cron from "node-cron";
import dotenv from "dotenv";
dotenv.config(); // Load environment variables (.env file)
import mysql from "mysql2/promise";

// --- Kubernetes Client ---
import * as k8s from "@kubernetes/client-node";

// --- Create Express App ---
const app = express();
app.use(express.json());


const pool = mysql.createPool({
  host: process.env.MYSQL_HOST || "database-1.cxyc4mq4ohwl.us-east-1.rds.amazonaws.com",
  port: process.env.MYSQL_PORT ? parseInt(process.env.MYSQL_PORT, 10) : 3306,
  user: process.env.MYSQL_USER || "stacknow_user",
  password: process.env.MYSQL_PASSWORD || "stacknow_password", // Set your password in .env
  database: process.env.MYSQL_DATABASE || "kube_stats_db",        // Ensure this database exists
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

const getDB = () => pool;


// --------------------------------------------------------------
// Kubernetes Client Initialization
// --------------------------------------------------------------
const kc = new k8s.KubeConfig();
let k8sCoreV1Api;
let k8sAppsV1Api;
let k8sCustomObjectsApi;
let k8sClientInitialized = false;

try {
    kc.loadFromDefault();
    k8sCoreV1Api = kc.makeApiClient(k8s.CoreV1Api);
    k8sAppsV1Api = kc.makeApiClient(k8s.AppsV1Api);
    k8sCustomObjectsApi = kc.makeApiClient(k8s.CustomObjectsApi);
    k8sClientInitialized = true;
    console.log("[INFO] Kubernetes client initialized successfully.");
} catch (err) {
    console.error("[FATAL] Failed to initialize Kubernetes client:", err.message);
    console.error("[FATAL] Kubernetes related features will be unavailable.");
    // Optionally exit: process.exit(1);
}

// --------------------------------------------------------------
// In-Memory Stats Buffer
// --------------------------------------------------------------
const statsBuffer = new Map(); // Key: "namespace/podName", Value: Array of stats objects

function addStatsToBuffer(podKey, stats) {
    if (!statsBuffer.has(podKey)) {
        statsBuffer.set(podKey, []);
    }
    statsBuffer.get(podKey).push(stats);
}

function getAndClearBuffer() {
    const data = new Map(statsBuffer);
    statsBuffer.clear();
    return data;
}

// --------------------------------------------------------------
// CPU/Memory Parsing Functions
// --------------------------------------------------------------
function parseCpu(cpuStr) {
    if (cpuStr === null || cpuStr === undefined) return 0;
    const str = String(cpuStr).trim();
    if (!str) return 0;
    if (str.endsWith("m")) { const num = parseFloat(str.slice(0, -1)); return !isNaN(num) ? num / 1000 : 0; }
    if (str.endsWith("u")) { const num = parseFloat(str.slice(0, -1)); return !isNaN(num) ? num / 1000000 : 0; }
    if (str.endsWith("n")) { const num = parseFloat(str.slice(0, -1)); return !isNaN(num) ? num / 1000000000 : 0; }
    const num = parseFloat(str);
    return !isNaN(num) ? num : 0;
}

function parseMemory(memStr) {
    if (memStr === null || memStr === undefined) return 0;
    const str = String(memStr).trim();
    if (!str) return 0;
    const regex = /^([0-9.]+)([eEinumkKMGTP]?i?)?$/;
    const match = str.match(regex);
    if (!match) { const num = parseFloat(str); return !isNaN(num) ? num : 0; }
    const value = parseFloat(match[1]);
    if (isNaN(value)) return 0;
    const unit = match[2] || '';
    switch (unit) {
        case 'Ki': return value * Math.pow(1024, 1);
        case 'Mi': return value * Math.pow(1024, 2);
        case 'Gi': return value * Math.pow(1024, 3);
        case 'Ti': return value * Math.pow(1024, 4);
        case 'Pi': return value * Math.pow(1024, 5);
        case 'Ei': return value * Math.pow(1024, 6);
        case 'k': return value * Math.pow(1000, 1);
        case 'M': return value * Math.pow(1000, 2);
        case 'G': return value * Math.pow(1000, 3);
        case 'T': return value * Math.pow(1000, 4);
        case 'P': return value * Math.pow(1000, 5);
        case 'E': return value * Math.pow(1000, 6);
        case 'm': return value * 1e-3;
        case 'u': return value * 1e-6;
        case 'n': return value * 1e-9;
        case '': default: return value;
    }
}

// --------------------------------------------------------------
// Kubernetes Stats Collection Logic
// --------------------------------------------------------------
async function collectStatsForAllPods() {
    if (!k8sClientInitialized) {
        console.warn("[WARN] Skipping stats collection: Kubernetes client not initialized.");
        return;
    }
    console.log(`[INFO] Starting K8s stats collection cycle at ${new Date().toISOString()}`);

    let podMetrics = [];
    let pods = [];
    let metricsOk = false;
    let specsOk = false;

    // --- 1. Get Pod Metrics (from metrics.k8s.io) ---
    try {
        const group = "metrics.k8s.io";
        const version = "v1beta1";
        const plural = "pods";
        console.log(`[DEBUG] K8s API Call: listClusterCustomObject (Group: ${group}, Version: ${version}, Plural: ${plural})`);
        const metricsRes = await k8sCustomObjectsApi.listClusterCustomObject({ group, version, plural });
        if (metricsRes && metricsRes.items && Array.isArray(metricsRes.items)) {
            podMetrics = metricsRes.items;
            metricsOk = true;
            console.log(`[INFO] K8s Metrics API: Fetched ${podMetrics.length} pod metric items.`);
        } else {
            console.warn("[WARN] K8s Metrics API: Response structure unexpected or empty. Body:", metricsRes);
        }
    } catch (err) {
        const statusCode = err.statusCode || 'N/A';
        if (statusCode === 404) {
            console.error("[ERROR] K8s Metrics API: Endpoint not found (404). Is Metrics Server installed?");
        } else if (statusCode === 403) {
            console.error(`[ERROR] K8s Metrics API: Permission denied (403). Check RBAC. Message: ${err.body?.message || err.message}`);
        } else {
            console.error(`[ERROR] K8s Metrics API: Failed to fetch pod metrics (Status: ${statusCode}): ${err.body?.message || err.message}`);
            if(err.response?.body) console.error("[ERROR] K8s Metrics API Response Body:", JSON.stringify(err.response.body));
        }
    }

    if (!metricsOk) {
        console.warn("[WARN] Aborting collection cycle due to Metrics API failure.");
        return;
    }

    // --- 2. Get Pod Specifications (from core/v1) ---
    try {
        console.log("[DEBUG] K8s API Call: listPodForAllNamespaces");
        const podsRes = await k8sCoreV1Api.listPodForAllNamespaces();
        if (podsRes && podsRes.items && Array.isArray(podsRes.items)) {
            pods = podsRes.items;
            specsOk = true;
            console.log(`[INFO] K8s Core API: Fetched ${pods.length} pod specifications.`);
        } else {
            console.error("[ERROR] K8s Core API: listPodForAllNamespaces response invalid. Body:", podsRes);
        }
    } catch (err) {
        const statusCode = err.statusCode || 'N/A';
        if (statusCode === 403) {
            console.error(`[ERROR] K8s Core API: Permission denied listing pods (403). Check RBAC. Message: ${err.body?.message || err.message}`);
        } else {
            console.error(`[ERROR] K8s Core API: Failed to fetch pod list (Status: ${statusCode}): ${err.body?.message || err.message}`);
            if(err.response) console.error("[ERROR] K8s Core API Response Body:", JSON.stringify(err.response));
        }
    }

    if (!specsOk) {
        console.warn("[WARN] Aborting collection cycle due to Core API failure (pod specs).");
        return;
    }

    // --- 3. Process and Combine Data ---
    const usageMap = new Map();
    podMetrics.forEach(metric => {
        const ns = metric.metadata?.namespace;
        const name = metric.metadata?.name;
        if (ns && name && Array.isArray(metric.containers)) {
            let cpu = 0, mem = 0;
            metric.containers.forEach(c => { cpu += parseCpu(c.usage?.cpu); mem += parseMemory(c.usage?.memory); });
            usageMap.set(`${ns}/${name}`, { cpuUsedCores: cpu, memUsedBytes: mem });
        }
    });
    console.log(`[DEBUG] Parsed metrics for ${usageMap.size} pods into usageMap.`);

    let processedCount = 0;
    for (const pod of pods) {
        const ns = pod.metadata?.namespace;
        const name = pod.metadata?.name;
        const nodeName = pod.spec?.nodeName || null;
        const phase = pod.status?.phase || null;
        if (!ns || !name) {
             console.warn("[WARN] Skipping pod spec with missing namespace or name:", pod.metadata?.uid);
             continue;
        }
        const podKey = `${ns}/${name}`;
        const usage = usageMap.get(podKey) || { cpuUsedCores: 0, memUsedBytes: 0 };

        let limitBytes = 0;
        if (pod.spec?.containers) {
            pod.spec.containers.forEach(c => { limitBytes += parseMemory(c.resources?.limits?.memory); });
        }

        const stats = {
            namespace: ns,
            podName: name,
            nodeName,
            phase,
            cpuUsage: usage.cpuUsedCores,
            memoryUsageMB: usage.memUsedBytes / (1024 * 1024),
            memoryUsagePercentage: limitBytes > 0 ? Math.max(0, (usage.memUsedBytes / limitBytes) * 100) : 0,
        };
        addStatsToBuffer(podKey, stats);
        processedCount++;
    }
    console.log(`[INFO] Finished processing and buffering stats for ${processedCount} pods.`);
}

// --------------------------------------------------------------
// Aggregation and Database Insertion Logic
// --------------------------------------------------------------
function aggregateStats(statsList) {
    const count = statsList.length;
    if (!count) return null;
    const { namespace, podName, nodeName, phase } = statsList[0];
    let cpuSum = 0, memMBSum = 0, memPctSum = 0;
    statsList.forEach(s => {
        cpuSum += s.cpuUsage || 0;
        memMBSum += s.memoryUsageMB || 0;
        memPctSum += s.memoryUsagePercentage || 0;
    });
    return {
        namespace, podName, nodeName, phase,
        cpuUsageAvg: cpuSum / count,
        memoryUsageMBAvg: memMBSum / count,
        memoryUsagePercentageAvg: memPctSum / count,
        dataPoints: count,
        aggregatedAt: new Date(),
    };
}

async function aggregateAndInsert() {
    const bufferedData = getAndClearBuffer();
    if (bufferedData.size === 0) {
        console.log("[DEBUG] No stats in buffer to aggregate.");
        return;
    }
    console.log(`[INFO] Aggregating stats for ${bufferedData.size} pod keys.`);

    const pool = getDB();
    let connection = null;
    const insertStmt = `
      INSERT INTO kube_pod_stats_aggregated
      (pod_name, namespace, node_name, phase, cpu_usage_avg, memory_usage_mb_avg, memory_usage_percentage_avg, data_points, aggregated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    try {
        connection = await pool.getConnection();
        await connection.beginTransaction();
        console.log("[DEBUG] Aggregation DB transaction started.");
        let insertedCount = 0;
        let failedCount = 0;
        for (const [podKey, statsList] of bufferedData.entries()) {
            const agg = aggregateStats(statsList);
            if (agg) {
                try {
                    const mysqlTimestamp = agg.aggregatedAt.toISOString().slice(0, 19).replace("T", " ");
                    await connection.query(insertStmt, [
                        agg.podName, agg.namespace, agg.nodeName, agg.phase, agg.cpuUsageAvg, agg.memoryUsageMBAvg,
                        agg.memoryUsagePercentageAvg, agg.dataPoints, mysqlTimestamp
                    ]);
                    insertedCount++;
                } catch (queryError) {
                    failedCount++;
                    console.error(`[ERROR] DB Insert failed for ${agg.namespace}/${agg.podName}: ${queryError.message} (SQLState: ${queryError.sqlState})`);
                }
            }
        }
        if (failedCount > 0) {
            console.warn(`[WARN] ${failedCount} aggregated records failed insertion. Committing ${insertedCount} successful records.`);
            await connection.commit();
        } else {
            await connection.commit();
            console.log(`[INFO] Aggregation DB transaction committed. Inserted ${insertedCount} records.`);
        }
    } catch (err) {
        console.error("[ERROR] Aggregation DB transaction error:", err.message);
        if (connection) {
            try { await connection.rollback(); console.log("[INFO] DB transaction rolled back due to main error."); }
            catch (rollbackErr) { console.error("[ERROR] Failed to rollback transaction:", rollbackErr.message); }
        }
    } finally {
        if (connection) {
            connection.release();
            console.log("[DEBUG] Aggregation DB connection released.");
        }
    }
}

// --------------------------------------------------------------
// Cron Scheduling
// --------------------------------------------------------------
// — at second 0, every 1 minute
const collectionCronExpr   = "0 * * * * *";   
// — at second 5, every 1 minute
const aggregationCronExpr  = "5 * * * * *";  

console.log("[INFO] Stats Collection Interval: 1 minute");
console.log("[INFO] Stats Aggregation Interval: 1 minute");


let isCollecting = false;
let isAggregating = false;

if (!cron.validate(collectionCronExpr)) {
    console.error(`[FATAL] Invalid cron expression for collection: "${collectionCronExpr}". Collector disabled.`);
} else {
    cron.schedule(collectionCronExpr, async () => {
        if (!k8sClientInitialized) return;
        if (isCollecting) {
            console.warn(`[WARN] Collection still running, skipping cycle at ${new Date().toISOString()}`);
            return;
        }
        isCollecting = true;
        try { await collectStatsForAllPods(); }
        catch (err) { console.error("[ERROR] Uncaught error in scheduled collection task:", err.message, err.stack); }
        finally { isCollecting = false; }
    });
}

if (!cron.validate(aggregationCronExpr)) {
    console.error(`[FATAL] Invalid cron expression for aggregation: "${aggregationCronExpr}". Aggregator disabled.`);
} else {
    cron.schedule(aggregationCronExpr, async () => {
        if (isAggregating) {
            console.warn(`[WARN] Aggregation still running, skipping cycle at ${new Date().toISOString()}`);
            return;
        }
        isAggregating = true;
        try { await aggregateAndInsert(); }
        catch (err) { console.error("[ERROR] Uncaught error in scheduled aggregation task:", err.message, err.stack); }
        finally { isAggregating = false; }
    });
}

// --------------------------------------------------------------
// Graceful Shutdown Handler
// --------------------------------------------------------------
async function gracefulShutdown(signal) {
    console.log(`\n[INFO] Received signal: ${signal}. Starting graceful shutdown...`);

    console.log("[INFO] Stopping cron jobs from scheduling new runs...");
    cron.getTasks().forEach(task => task.stop());

    isCollecting = true;
    isAggregating = true;

    console.log("[INFO] Allowing brief time for ongoing operations (2s)...");
    await new Promise(resolve => setTimeout(resolve, 2000));

    console.log("[INFO] Performing final aggregation and DB insertion...");
    try {
        await aggregateAndInsert();
        console.log("[INFO] Final aggregation complete.");
    } catch (err) {
        console.error("[ERROR] Error during final aggregation on shutdown:", err.message);
    }

    console.log("[INFO] Closing database pools...");
    try {
        const poolAgg = getDB();
        await Promise.all([poolAgg.end()]);
        console.log("[INFO] Database pools closed.");
    } catch (dbErr) {
        console.error("[ERROR] Error closing database pools:", dbErr.message);
    }

    console.log("[INFO] Shutdown complete. Exiting.");
    process.exit(0);
}

process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

// --------------------------------------------------------------
// Start the Server
// --------------------------------------------------------------
const PORT = process.env.PORT || 5200;
app.listen(PORT, () => {
    console.log(`[INFO] K8s Pod Stats Server is running on port ${PORT}`);
});
