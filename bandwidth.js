/**
 * File: bandwidthService.js
 * Description:
 *   - Collects bandwidth metrics for Kubernetes pods with an istio-proxy.
 *   - Uses @kubernetes/client-node for API interactions.
 *   - Buffers previous metrics to compute deltas.
 *   - Inserts metrics into a MySQL database.
 *   - Provides an API endpoint to query stored bandwidth metrics.
 * Prerequisite: Project's package.json MUST include "type": "module"
 * Prerequisite: Target pods must have an 'istio-proxy' container exposing metrics on port 15000.
 * Prerequisite: utils/k8sClient.js and utils/db.js are correctly set up.
 */

// --- Core Dependencies ---
import express from 'express';
import cron from 'node-cron';
import dotenv from 'dotenv';
dotenv.config();
import { Writable } from 'stream';

// --- Kubernetes Client Imports ---
import {
    initKubernetesClients, // Your utility to initialize all K8s clients
    coreV1Api,             // Exported CoreV1Api instance from your utility
    kc                     // Exported KubeConfig instance from your utility
} from './utils/k8sClient.js';
import { Exec } from '@kubernetes/client-node';

// --- Application Utilities & Middleware ---
import { metricsDb } from "./utils/db.js";

// --- In-Memory Cache & Configuration ---
const prevMetrics = new Map(); // Key: "<namespace>:<pod_name>", Value: { rx: number, tx: number, timestamp: number }

const excludedNamespaces = new Set([
  'default',
  'falco',
  'istio-system',
  'kube-node-lease',
  'kube-public',
  'kube-system',
  'nfs-provisioner',
  'gmp-system',
  'gke-managed-cim',
]);

const METRICS_URL = process.env.ISTIO_METRICS_URL || 'http://localhost:15000/stats';
const RX_METRIC_REGEX = /upstream_cx_rx_bytes_total(?:\{[^}]*\})?:\s*(\d+)/;
const TX_METRIC_REGEX = /upstream_cx_tx_bytes_total(?:\{[^}]*\})?:\s*(\d+)/;

// --- Kubernetes Client Initialization State ---
let k8sClientInitialized = false;
let dbConnectionVerified = false;

(async () => {
    try {
        await initKubernetesClients();
        if (coreV1Api && kc) {
            k8sClientInitialized = true;
            console.log("[INFO] Kubernetes clients initialized successfully for bandwidthService.");
            const currentContext = kc.getCurrentContext();
            const cluster = kc.getCluster(kc.getContextObject(currentContext)?.cluster || '');
            console.log(`[INFO] K8s Context: ${currentContext}, Server: ${cluster?.server}`);

            // Verify DB connection after K8s init (or could be parallel)
            try {
                const [rows] = await metricsDb.query("SELECT 1 AS db_check");
                if (rows && rows[0] && rows[0].db_check === 1) {
                    dbConnectionVerified = true;
                    console.log("[INFO] Database connection verified successfully.");
                } else {
                    console.error("[ERROR] Database connection check failed (unexpected result).");
                }
            } catch (dbErr) {
                console.error("[FATAL_DB] Failed to connect/query the database on startup:", dbErr.message, dbErr.code ? `(Code: ${dbErr.code})` : '');
            }

        } else {
            throw new Error("CoreV1Api or KubeConfig (kc) is undefined after initialization from utils/k8sClient.js.");
        }
    } catch (error) {
        console.error("[FATAL_K8S_INIT] Failed to initialize Kubernetes clients in bandwidthService:", error.message, error.stack);
    }
})();


// --- Bandwidth Metrics Collection Function ---
async function collectMetrics() {
  const collectionTimestamp = new Date();
  console.log(`[${collectionTimestamp.toISOString()}] Starting bandwidth metrics collection cycle...`);

  if (!k8sClientInitialized) {
    console.warn(`[${collectionTimestamp.toISOString()}] Skipping bandwidth collection: Kubernetes client not initialized.`);
    return;
  }
  if (!coreV1Api || !kc) { // Should be covered by k8sClientInitialized
    console.warn(`[${collectionTimestamp.toISOString()}] Skipping bandwidth collection: coreV1Api or kc is not available.`);
    return;
  }
  if (!dbConnectionVerified) {
    console.warn(`[${collectionTimestamp.toISOString()}] Skipping bandwidth collection: Database connection not verified.`);
    return;
  }


  let podItems = [];
  try {
    const podsRes = await coreV1Api.listPodForAllNamespaces(
        undefined, undefined, "status.phase=Running", undefined, undefined, undefined, undefined, undefined, false, undefined, false
    );

    if (podsRes.response && podsRes.response && Array.isArray(podsRes.response.items)) {
        podItems = podsRes.response.items;
    } else if (podsRes && Array.isArray(podsRes.items)) {
        podItems = podsRes.items;
    } else if (Array.isArray(podsRes.items)) { // Less common, direct items
        podItems = podsRes.items;
    } else {
        console.warn(`[${collectionTimestamp.toISOString()}] No pod items found or unexpected K8s API response structure. Keys: ${Object.keys(podsRes || {}).join(', ')}. Resp (500chars):`, JSON.stringify(podsRes).substring(0,500));
        return;
    }
  } catch (err) {
    let errorMessage = "Unknown error fetching pod list";
    let statusCode = 'N/A';
    let errorDetails = null;
    if (err.response) {
      statusCode = err.response.statusCode;
      if (err.response && typeof err.response === 'object') {
          errorMessage = err.response.message || JSON.stringify(err.response);
          errorDetails = err.response;
      } else if (err.response.data && typeof err.response.data === 'object') {
          errorMessage = err.response.data.message || JSON.stringify(err.response.data);
          errorDetails = err.response.data;
      } else if (typeof err.response === 'string') {
          errorMessage = err.response; errorDetails = err.response;
          try { const parsed = JSON.parse(err.response); errorMessage = parsed.message || err.response; errorDetails = parsed; } catch(e) { /* ignore */ }
      } else { errorMessage = err.response.statusMessage || `HTTP Error ${statusCode}`; }
    } else if (err.message) { errorMessage = err.message; }
    console.error(`[${collectionTimestamp.toISOString()}] Error fetching pod list (Status: ${statusCode}):`, errorMessage);
    if (errorDetails) console.error("[ERROR_DETAIL_PODLIST] K8s API Response:", JSON.stringify(errorDetails));
    else if (err.stack) console.error("[ERROR_STACK_PODLIST]", err.stack);
    return;
  }

  console.log(`[${collectionTimestamp.toISOString()}] Found ${podItems.length} running pods to process.`);
  if (podItems.length === 0) return; // Nothing to do

  let processedCount = 0;
  let errorCount = 0;

  const MAX_CONCURRENT_POD_PROCESSING = parseInt(process.env.MAX_CONCURRENT_BANDWIDTH_PODS || "10", 10);
  const podProcessingPromises = [];

  for (const pod of podItems) {
    podProcessingPromises.push(
      (async () => {
        const namespace = pod.metadata?.namespace;
        const podName = pod.metadata?.name;
        let podKey = `${namespace || 'unknown_ns'}/${podName || 'unknown_pod'}`; // podKey defined early for logging

        if (!namespace || !podName) {
          console.warn(`[DEBUG_POD_SKIP] ${podKey} - Skipping pod with missing metadata: UID ${pod.metadata?.uid}`);
          return { status: 'skipped', podKey, reason: 'missing metadata' };
        }
        // Re-assign podKey with actual values
        podKey = `${namespace}:${podName}`;


        if (excludedNamespaces.has(namespace)) {
          // console.debug(`[DEBUG_POD_SKIP] ${podKey} - Excluded namespace.`);
          return { status: 'skipped', podKey, reason: 'excluded namespace' };
        }

        const istioProxyContainer = pod.spec?.containers?.find(c => c.name === 'istio-proxy');
        if (!istioProxyContainer) {
          // console.debug(`[DEBUG_POD_SKIP] ${podKey} - No 'istio-proxy' container.`);
          return { status: 'skipped', podKey, reason: 'no istio-proxy container' };
        }

        const k8sExec = new Exec(kc);
        let metricsOutput = '';
        let commandErrorOutput = '';
        
        const stdoutHandler = new Writable({
          write(chunk, encoding, callback) {
            metricsOutput += chunk.toString();
            callback();
          },
        });
        
        const stderrHandler = new Writable({
          write(chunk, encoding, callback) {
            commandErrorOutput += chunk.toString();
            callback();
          },
        });

        try {
          // console.log(`[DEBUG_POD] ${podKey} - Attempting exec...`);
          const command = ['curl', '-s', '--max-time', '5', METRICS_URL];

          await new Promise((resolve, reject) => {
            const execPromise = k8sExec.exec(
              namespace, podName, 'istio-proxy', command,
              stdoutHandler, stderrHandler, null, false,
              (status) => { // This status is for the WebSocket stream setup by K8s API
                if (status.status === 'Failure') {
                    // This usually means K8s API itself failed to establish the exec session
                    // e.g. pod not found, pod terminating, container not ready/found, RBAC error for exec
                    // The actual error message from K8s should be in status.message or status.details
                    // commandErrorOutput might also catch a message from the K8s execshim in the node.
                    const k8sApiExecError = status.message || JSON.stringify(status.details) || "K8s API exec setup failure";
                    console.warn(`[DEBUG_POD_EXEC_SETUP_FAILURE] ${podKey} - K8s API status: Failure. Message: ${k8sApiExecError}. Current commandStderr: "${commandErrorOutput.trim()}"`);
                    // We'll let onclose/onerror handle the final rejection, as stderr might provide more context
                    // from the node/container runtime if the command even started to try.
                }
              }
            );

            execPromise.then(ws => {
              ws.onclose = () => {
                // console.log(`[DEBUG_POD] ${podKey} - WebSocket closed. Stderr: "${commandErrorOutput.trim()}", Stdout (len): ${metricsOutput.length}`);
                if (commandErrorOutput) { // If curl or any command writes to stderr
                  reject(new Error(`Command in ${podKey} failed with stderr: ${commandErrorOutput.trim()}`));
                } else if (metricsOutput.trim() === '') { // Curl succeeded but no output
                   if (commandErrorOutput.includes("Connection refused") || commandErrorOutput.includes("Could not resolve host")) {
                        reject(new Error(`Command in ${podKey} likely failed (curl error on empty stdout): ${commandErrorOutput.trim()}`));
                   } else {
                        // console.warn(`[DEBUG_POD] ${podKey} - Empty metrics output with no stderr. Curl might have failed silently or no metrics available.`);
                        resolve();
                   }
                } else { // Curl succeeded with output
                  resolve();
                }
              };
              ws.onerror = (err) => {
                console.error(`[DEBUG_POD_WEBSOCKET_ERROR] ${podKey} - WebSocket error: ${err.message}. Command Stderr: "${commandErrorOutput.trim()}"`);
                reject(new Error(`K8s exec WebSocket error for ${podKey}: ${err.message}. Current commandStderr: "${commandErrorOutput.trim()}"`));
              };
            }).catch(err => { // Error from k8sExec.exec() promise itself (e.g. initial connection failed)
              console.error(`[DEBUG_POD_EXEC_PROMISE_REJECT] ${podKey} - Exec setup promise rejected: ${err.message}. Command Stderr: "${commandErrorOutput.trim()}"`);
              reject(new Error(`K8s exec setup failed for ${podKey}: ${err.message}. Current commandStderr: "${commandErrorOutput.trim()}"`));
            });
          });

          // console.log(`[DEBUG_POD] ${podKey} - Exec finished. Stdout (first 100): "${metricsOutput.substring(0,100)}". Stderr: "${commandErrorOutput.trim()}"`);

          const rxMatch = metricsOutput.match(RX_METRIC_REGEX);
          const txMatch = metricsOutput.match(TX_METRIC_REGEX);

          if (!rxMatch || !txMatch) {
            console.warn(`[DEBUG_POD_METRICS_FAIL] ${podKey} - Regex match failed. Output (first 300 chars): ${metricsOutput.substring(0,300)}`);
            if (commandErrorOutput) console.warn(`[DEBUG_POD_METRICS_FAIL] ${podKey} - Stderr was: ${commandErrorOutput.trim()}`);
            return { status: 'error', podKey, reason: 'metrics_not_found_in_output' };
          }

          // console.log(`[DEBUG_POD] ${podKey} - Metrics regex matched. RX: ${rxMatch[1]}, TX: ${txMatch[1]}`);

          const currentRx = parseInt(rxMatch[1], 10);
          const currentTx = parseInt(txMatch[1], 10);
          const previous = prevMetrics.get(podKey) || { rx: 0, tx: 0, timestamp: 0 };
          let deltaRx = currentRx - previous.rx;
          let deltaTx = currentTx - previous.tx;
          if (deltaRx < 0) { console.warn(`[${collectionTimestamp.toISOString()}] RX counter reset for ${podKey}. Old: ${previous.rx}, New: ${currentRx}.`); deltaRx = currentRx; }
          if (deltaTx < 0) { console.warn(`[${collectionTimestamp.toISOString()}] TX counter reset for ${podKey}. Old: ${previous.tx}, New: ${currentTx}.`); deltaTx = currentTx; }
          prevMetrics.set(podKey, { rx: currentRx, tx: currentTx, timestamp: Date.now() });

          // console.log(`[DEBUG_POD] ${podKey} - Attempting DB insert...`);
          await metricsDb.execute(
            `INSERT INTO bandwidth_metrics (timestamp, namespace, pod_name, incoming_bandwidth_total, outgoing_bandwidth_total, incoming_delta, outgoing_delta) VALUES (?, ?, ?, ?, ?, ?, ?)`,
            [collectionTimestamp, namespace, podName, currentRx, currentTx, deltaRx, deltaTx]
          );
          // console.log(`[DEBUG_POD] ${podKey} - DB insert successful.`);
          return { status: 'success', podKey };

        } catch (podErr) {
          const errMsg = podErr.message || "Unknown pod processing error";
          console.error( // More detailed log for any caught error during pod processing
            `[CRITICAL_POD_FAILURE] Pod: ${podKey}. Reason: "${errMsg}".`,
            // `\nFull Error: ${podErr.message}`, // Already in errMsg
            podErr.stack ? `\nStack: ${podErr.stack}` : '',
            commandErrorOutput ? `\nCaptured Command Stderr: "${commandErrorOutput.trim()}"` : '',
            metricsOutput ? `\nCaptured Command Stdout (len ${metricsOutput.length}, first 300 chars): "${metricsOutput.substring(0,300)}..."` : ''
          );
          return { status: 'error', podKey, reason: errMsg.substring(0,250) };
        }
      })()
    );
  } // End for...of podItems

  for (let i = 0; i < podProcessingPromises.length; i += MAX_CONCURRENT_POD_PROCESSING) {
    const batch = podProcessingPromises.slice(i, i + MAX_CONCURRENT_POD_PROCESSING);
    const results = await Promise.allSettled(batch);
    results.forEach(result => {
      if (result.status === 'fulfilled') {
        if (result.value?.status === 'success') processedCount++;
        else if (result.value?.status === 'error') {
            errorCount++;
            // console.warn(`[AGGREGATED_ERROR] Pod processing failed for ${result.value.podKey}. Reason: ${result.value.reason}`);
        } else if (result.value?.status === 'skipped') {
            // console.debug(`[AGGREGATED_SKIP] Pod ${result.value.podKey} skipped. Reason: ${result.value.reason}`);
        }
      } else { // Promise was rejected (should ideally be caught within the IIFE and returned as status:'error')
        errorCount++;
        console.error(`[${collectionTimestamp.toISOString()}] UNHANDLED_REJECTION in pod processing batch for a pod. Reason:`, result.reason);
      }
    });
  }

  console.log(`[${collectionTimestamp.toISOString()}] Bandwidth metrics collection cycle finished. Processed: ${processedCount}, Errors: ${errorCount}, Skipped: ${podItems.length - processedCount - errorCount}.`);

  const now = Date.now();
  const cleanupThreshold = 2 * 60 * 60 * 1000;
  for (const [key, value] of prevMetrics.entries()) {
    if (now - value.timestamp > cleanupThreshold) {
      prevMetrics.delete(key);
    }
  }
}

// --------------------------------------------------------------
// Cron Scheduling for Metrics Collection
// --------------------------------------------------------------
const cronSchedule = process.env.BANDWIDTH_CRON_SCHEDULE || '0 * * * * *';
if (!cron.validate(cronSchedule)) {
  console.error(`[FATAL] Invalid cron schedule: "${cronSchedule}". Bandwidth metrics collection will not run.`);
} else {
  cron.schedule(cronSchedule, () => {
    if (!k8sClientInitialized || !dbConnectionVerified) { // Check both
        console.warn(`[${new Date().toISOString()}] Cron: Skipping bandwidth collection, K8s client or DB not ready (K8s: ${k8sClientInitialized}, DB: ${dbConnectionVerified}).`);
        return;
    }
    if (collectMetrics.isRunning) {
      console.warn(`[${new Date().toISOString()}] Bandwidth collection is already running. Skipping this cycle.`);
      return;
    }
    collectMetrics.isRunning = true;
    collectMetrics().catch(err => { // Catch errors from the task itself
      console.error(`[${new Date().toISOString()}] Uncaught error in scheduled collectMetrics task:`, err);
    }).finally(() => {
      collectMetrics.isRunning = false;
    });
  });
  console.log(`[INFO] Bandwidth metrics collection scheduled with cron: "${cronSchedule}" (runs every minute)`);
}

// --------------------------------------------------------------
// Create Express App and API Endpoint
// --------------------------------------------------------------
const app = express();
app.use(express.json());

app.get('/healthz', (req, res) => {
  res.status(200).json({ 
    status: "ok", 
    k8s_client_initialized: k8sClientInitialized, 
    db_connection_verified: dbConnectionVerified,
    timestamp: new Date().toISOString() 
  });
});

app.get('/api/bandwidth', async (req, res) => {
  const { namespace, pod_name, limit = 100, hours_ago = 24 } = req.query;
  if (!dbConnectionVerified) {
    return res.status(503).json({ error: "Database not available."});
  }
  try {
    let query = `
      SELECT timestamp, namespace, pod_name, incoming_bandwidth_total, outgoing_bandwidth_total, incoming_delta, outgoing_delta
      FROM bandwidth_metrics
      WHERE timestamp >= NOW() - INTERVAL ? HOUR
    `;
    const params = [parseInt(hours_ago, 10) || 24];

    if (namespace) { query += ' AND namespace = ?'; params.push(namespace); }
    if (pod_name) { query += ' AND pod_name = ?'; params.push(pod_name); }
    query += ' ORDER BY timestamp DESC LIMIT ?';
    params.push(parseInt(limit, 10) || 100);

    const [rows] = await metricsDb.query(query, params);
    res.json(rows);
  } catch (error) {
    console.error(`[API_ERROR] /api/bandwidth: ${error.message}`);
    res.status(500).json({ error: 'Failed to retrieve bandwidth metrics' });
  }
});

// --------------------------------------------------------------
// Start the Server
// --------------------------------------------------------------
const PORT = process.env.BANDWIDTH_SERVICE_PORT || process.env.PORT || 3000;
const HOST = "0.0.0.0";

app.listen(PORT, HOST, () => {
  console.log(`[INFO] Bandwidth Metrics Service is running on port ${PORT}`);
  if (!k8sClientInitialized) {
    console.error("[STARTUP_ERROR] K8s client FAILED TO INITIALIZE. Metrics collection will not work.");
  }
  if (!dbConnectionVerified) {
    console.error("[STARTUP_ERROR] Database connection FAILED TO VERIFY. Metrics collection might not work or API will fail.");
  }
});

// Graceful shutdown
async function gracefulShutdown(signal) {
    console.log(`\n[INFO] Received signal: ${signal}. Starting graceful shutdown for Bandwidth Service...`);
    global.isShuttingDown = true;
    console.log("[INFO] Stopping cron jobs from scheduling new runs...");
    cron.getTasks().forEach(task => task.stop());

    if (collectMetrics.isRunning) {
        console.log("[INFO] Waiting for ongoing collection to finish (max 30s)...");
        await new Promise(resolve => {
            const timeout = setTimeout(() => {
                console.warn("[WARN] Ongoing collection did not finish in 30s. Proceeding with shutdown.");
                resolve();
            }, 30000);
            const interval = setInterval(() => {
                if (!collectMetrics.isRunning) { clearTimeout(timeout); clearInterval(interval); resolve(); }
            }, 200);
        });
    }

    console.log("[INFO] Closing database pool (metricsDb)...");
    try {
        if (metricsDb && typeof metricsDb.end === 'function') { // Check if pool exists and has end method
            await metricsDb.end();
            console.log("[INFO] Database pool closed.");
        } else {
            console.log("[INFO] Database pool (metricsDb) was not initialized or already closed.");
        }
    } catch (dbErr) {
        console.error("[ERROR] Error closing database pool:", dbErr.message);
    }

    console.log("[INFO] Bandwidth Service shutdown complete. Exiting.");
    process.exit(0);
}

process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));