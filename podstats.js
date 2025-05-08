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
// mysql is imported in db.js, no need here if only using the pool from there

// --- Create Express App ---
const app = express();
app.use(express.json());

// --- Database Utility ---
// metricsDb is the mysql2/promise pool instance
import { metricsDb } from "./utils/db.js";

// --------------------------------------------------------------
// Kubernetes Client Initialization
// --------------------------------------------------------------
import {
    initKubernetesClients,
    coreV1Api,
    // appsV1Api, // Not used in this script snippet
    // networkingV1Api, // Not used
    // policyV1Api, // Not used
    customObjectsApi, // Assuming k8sClient.js exports this
    // kc, // KubeConfig instance, not directly used here if APIs are pre-made
  } from './utils/k8sClient.js';

let k8sClientInitialized = false;
try {
  await initKubernetesClients();
  // Check if APIs are actually available (depends on k8sClient.js implementation)
  if (coreV1Api && customObjectsApi) {
    k8sClientInitialized = true;
    console.log("[INFO] Kubernetes clients initialized successfully.");
  } else {
    throw new Error("One or more Kubernetes API clients are undefined after initialization.");
  }
} catch (error) {
    console.error("[FATAL] Failed to initialize Kubernetes clients:", error.message, error.stack);
    // Depending on requirements, you might want to exit if K8s client is critical
    // process.exit(1);
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
    if (!match) { const num = parseFloat(str); return !isNaN(num) ? num : 0; } // Fallback for plain numbers
    const value = parseFloat(match[1]);
    if (isNaN(value)) return 0;
    const unit = match[2] || '';
    switch (unit) {
        case 'Ki': return value * Math.pow(1024, 1);
        case 'Mi': return value * Math.pow(1024, 2);
        case 'Gi': return value * Math.pow(1024, 3);
        case 'Ti': return value * Math.pow(1024, 4);
        case 'Pi': return value * Math.pow(1024, 5);
        case 'Ei': return value * Math.pow(1024, 6); // peta, exa
        case 'k': return value * Math.pow(1000, 1); // kilo
        case 'M': return value * Math.pow(1000, 2); // mega
        case 'G': return value * Math.pow(1000, 3); // giga
        case 'T': return value * Math.pow(1000, 4); // tera
        case 'P': return value * Math.pow(1000, 5); // peta
        case 'E': return value * Math.pow(1000, 6); // exa
        case 'm': return value * 1e-3; // milli
        case 'u': return value * 1e-6; // micro
        case 'n': return value * 1e-9; // nano
        case '': default: return value; // Bytes
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

    let podMetricsItems = [];
    let podSpecItems = [];
    let metricsOk = false;
    let specsOk = false;

    // --- 1. Get Pod Metrics (from metrics.k8s.io) ---
    try {
        const group = "metrics.k8s.io";
        const version = "v1beta1";
        const plural = "pods";
        console.log(`[DEBUG] K8s API Call: listClusterCustomObject (Group: ${group}, Version: ${version}, Plural: ${plural})`);
        // Corrected API call: arguments are passed directly, not as an object
        // The response object from client-node typically has { response: http.IncomingMessage, body: ParsedType }
        const metricsRes = await customObjectsApi.listClusterCustomObject(group, version, plural);
        if (metricsRes.body && metricsRes.body.items && Array.isArray(metricsRes.body.items)) {
            podMetricsItems = metricsRes.body.items;
            metricsOk = true;
            console.log(`[INFO] K8s Metrics API: Fetched ${podMetricsItems.length} pod metric items.`);
        } else {
            console.warn("[WARN] K8s Metrics API: Response structure unexpected or empty. Body:", JSON.stringify(metricsRes.body));
        }
    } catch (err) {
        const statusCode = err.response?.statusCode || 'N/A';
        const errorMessage = err.body?.message || err.message;
        if (statusCode === 404) {
            console.error("[ERROR] K8s Metrics API: Endpoint not found (404). Is Metrics Server installed and API service registered?");
        } else if (statusCode === 403) {
            console.error(`[ERROR] K8s Metrics API: Permission denied (403). Check RBAC. Message: ${errorMessage}`);
        } else {
            console.error(`[ERROR] K8s Metrics API: Failed to fetch pod metrics (Status: ${statusCode}): ${errorMessage}`);
        }
        if(err.response?.body) console.error("[ERROR_DETAIL] K8s Metrics API Response Body:", JSON.stringify(err.response.body));
    }

    if (!metricsOk) {
        console.warn("[WARN] Aborting collection cycle due to Metrics API failure.");
        return;
    }

    // --- 2. Get Pod Specifications (from core/v1) ---
    try {
        console.log("[DEBUG] K8s API Call: listPodForAllNamespaces");
        const podsRes = await coreV1Api.listPodForAllNamespaces();
        if (podsRes.body && podsRes.body.items && Array.isArray(podsRes.body.items)) {
            podSpecItems = podsRes.body.items;
            specsOk = true;
            console.log(`[INFO] K8s Core API: Fetched ${podSpecItems.length} pod specifications.`);
        } else {
            console.error("[ERROR] K8s Core API: listPodForAllNamespaces response invalid. Body:", JSON.stringify(podsRes.body));
        }
    } catch (err) {
        const statusCode = err.response?.statusCode || 'N/A';
        const errorMessage = err.body?.message || err.message;
        if (statusCode === 403) {
            console.error(`[ERROR] K8s Core API: Permission denied listing pods (403). Check RBAC. Message: ${errorMessage}`);
        } else {
            console.error(`[ERROR] K8s Core API: Failed to fetch pod list (Status: ${statusCode}): ${errorMessage}`);
        }
        if(err.response?.body) console.error("[ERROR_DETAIL] K8s Core API Response Body:", JSON.stringify(err.response.body));
    }

    if (!specsOk) {
        console.warn("[WARN] Aborting collection cycle due to Core API failure (pod specs).");
        return;
    }

    // --- 3. Process and Combine Data ---
    const usageMap = new Map();
    podMetricsItems.forEach(metric => {
        const ns = metric.metadata?.namespace;
        const name = metric.metadata?.name;
        if (ns && name && Array.isArray(metric.containers)) {
            let cpu = 0, mem = 0;
            metric.containers.forEach(c => {
                cpu += parseCpu(c.usage?.cpu);
                mem += parseMemory(c.usage?.memory);
            });
            usageMap.set(`${ns}/${name}`, { cpuUsedCores: cpu, memUsedBytes: mem });
        }
    });
    console.log(`[DEBUG] Parsed metrics for ${usageMap.size} pods into usageMap.`);

    let processedCount = 0;
    for (const pod of podSpecItems) {
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
            pod.spec.containers.forEach(c => {
                if (c.resources?.limits?.memory) {
                    limitBytes += parseMemory(c.resources.limits.memory);
                }
            });
        }

        const stats = {
            namespace: ns,
            podName: name,
            nodeName,
            phase,
            cpuUsage: usage.cpuUsedCores,
            memoryUsageMB: usage.memUsedBytes / (1024 * 1024), // Convert bytes to MB
            memoryUsagePercentage: limitBytes > 0 ? Math.max(0, Math.min(100, (usage.memUsedBytes / limitBytes) * 100)) : 0,
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
    if (!statsList || statsList.length === 0) return null;

    const count = statsList.length;
    const { namespace, podName, nodeName, phase } = statsList[0]; // Assume these are constant for a given podKey over the aggregation interval

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
        aggregatedAt: new Date(), // Use JS Date object
    };
}

async function aggregateAndInsert() {
    const bufferedData = getAndClearBuffer();
    if (bufferedData.size === 0) {
        console.log("[DEBUG] No stats in buffer to aggregate.");
        return;
    }
    console.log(`[INFO] Aggregating stats for ${bufferedData.size} pod keys.`);

    let connection = null;
    const insertStmt = `
      INSERT INTO kube_pod_stats_aggregated
      (pod_name, namespace, node_name, phase, cpu_usage_avg, memory_usage_mb_avg, memory_usage_percentage_avg, data_points, aggregated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    try {
        connection = await metricsDb.getConnection(); // Use metricsDb directly
        await connection.beginTransaction();
        console.log("[DEBUG] Aggregation DB transaction started.");

        let insertedCount = 0;
        let failedCount = 0;

        for (const [_podKey, statsList] of bufferedData.entries()) {
            const agg = aggregateStats(statsList);
            if (agg) {
                try {
                    // Convert JS Date to MySQL DATETIME format 'YYYY-MM-DD HH:MM:SS'
                    const mysqlTimestamp = agg.aggregatedAt.toISOString().slice(0, 19).replace("T", " ");
                    await connection.query(insertStmt, [
                        agg.podName, agg.namespace, agg.nodeName, agg.phase,
                        agg.cpuUsageAvg, agg.memoryUsageMBAvg, agg.memoryUsagePercentageAvg,
                        agg.dataPoints, mysqlTimestamp
                    ]);
                    insertedCount++;
                } catch (queryError) {
                    failedCount++;
                    console.error(`[ERROR] DB Insert failed for ${agg.namespace}/${agg.podName}: ${queryError.message} (SQLState: ${queryError.sqlState})`);
                    // Consider if you want to log queryError.sql for debugging specific SQL issues
                }
            }
        }

        if (failedCount > 0) {
            // Decide on commit/rollback strategy for partial failures.
            // Current: Commit successful ones, log failures.
            console.warn(`[WARN] ${failedCount} aggregated records failed insertion. Committing ${insertedCount} successful records.`);
            await connection.commit();
        } else {
            await connection.commit();
            console.log(`[INFO] Aggregation DB transaction committed. Inserted ${insertedCount} records.`);
        }

    } catch (err) {
        console.error("[ERROR] Aggregation DB transaction error:", err.message, err.stack);
        if (connection) {
            try {
                await connection.rollback();
                console.log("[INFO] DB transaction rolled back due to main error.");
            } catch (rollbackErr) {
                console.error("[ERROR] Failed to rollback transaction:", rollbackErr.message);
            }
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
// Collect every minute at second 0
const collectionCronExpr   = "0 * * * * *";
// Aggregate every minute at second 5 (giving a few seconds for collection to finish)
const aggregationCronExpr  = "5 * * * * *";

console.log(`[INFO] Stats Collection Cron: "${collectionCronExpr}"`);
console.log(`[INFO] Stats Aggregation Cron: "${aggregationCronExpr}"`);

let isCollecting = false;
let isAggregating = false;

if (!cron.validate(collectionCronExpr)) {
    console.error(`[FATAL] Invalid cron expression for collection: "${collectionCronExpr}". Collector disabled.`);
} else {
    cron.schedule(collectionCronExpr, async () => {
        if (!k8sClientInitialized) {
            console.warn("[WARN] Cron Collection: Skipping, K8s client not ready.");
            return;
        }
        if (isCollecting) {
            console.warn(`[WARN] Collection still running, skipping cycle at ${new Date().toISOString()}`);
            return;
        }
        isCollecting = true;
        try {
            await collectStatsForAllPods();
        } catch (err) { // Catch errors from the task itself
            console.error("[ERROR] Uncaught error in scheduled collection task:", err.message, err.stack);
        } finally {
            isCollecting = false;
        }
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
        try {
            await aggregateAndInsert();
        } catch (err) { // Catch errors from the task itself
            console.error("[ERROR] Uncaught error in scheduled aggregation task:", err.message, err.stack);
        } finally {
            isAggregating = false;
        }
    });
}

// --------------------------------------------------------------
// Graceful Shutdown Handler
// --------------------------------------------------------------
async function gracefulShutdown(signal) {
    console.log(`\n[INFO] Received signal: ${signal}. Starting graceful shutdown...`);

    console.log("[INFO] Stopping cron jobs from scheduling new runs...");
    cron.getTasks().forEach(task => task.stop());

    // Prevent new operations from starting
    isCollecting = true; // Mark as busy
    isAggregating = true; // Mark as busy
    // Note: This doesn't stop an *already running* collectStatsForAllPods or aggregateAndInsert.
    // The timeout below is a simple way to allow them some time.
    // More robust would be to have these functions check a global shutdown flag.

    console.log("[INFO] Allowing brief time for ongoing operations (e.g., 5s)...");
    await new Promise(resolve => setTimeout(resolve, 5000)); // Increased timeout slightly

    console.log("[INFO] Performing final aggregation and DB insertion if any data remains...");
    try {
        await aggregateAndInsert(); // Process any remaining data in buffer
        console.log("[INFO] Final aggregation attempt complete.");
    } catch (err) {
        console.error("[ERROR] Error during final aggregation on shutdown:", err.message);
    }

    console.log("[INFO] Closing database pool (metricsDb)...");
    try {
        await metricsDb.end(); // Close the specific pool
        // If mainDb were used by this script, close it too: await mainDb.end();
        console.log("[INFO] Database pool closed.");
    } catch (dbErr) {
        console.error("[ERROR] Error closing database pool:", dbErr.message);
    }

    console.log("[INFO] Shutdown complete. Exiting.");
    process.exit(0);
}

process.on("SIGINT", () => gracefulShutdown("SIGINT")); // Ctrl+C
process.on("SIGTERM", () => gracefulShutdown("SIGTERM")); // Kubernetes termination signal

// --------------------------------------------------------------
// API Endpoints (Placeholder - Not implemented in original snippet)
// --------------------------------------------------------------
// Example:
// app.get("/api/aggregated-stats", async (req, res) => { /* ... query DB ... */ });
// app.get("/api/allocated-resources", async (req, res) => { /* ... query K8s specs ... */ });


// --------------------------------------------------------------
// Start the Server
// --------------------------------------------------------------
const PORT = process.env.PORT || 5200;
app.listen(PORT, () => {
    if (k8sClientInitialized) {
        console.log(`[INFO] K8s Pod Stats Server is running on port ${PORT}`);
    } else {
        console.error(`[WARN] K8s Pod Stats Server is running on port ${PORT}, BUT KUBERNETES CLIENT FAILED TO INITIALIZE. Stats collection will not work.`);
    }
});