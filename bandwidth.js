/**
 * File: bandwidthService.js
 * Description:
 *   - Collects bandwidth metrics for Kubernetes pods.
 *   - Buffers previous metrics to compute deltas.
 *   - Inserts metrics into a MySQL database.
 *   - Provides an API endpoint to query stored bandwidth metrics.
 * Prerequisite: Project's package.json MUST include "type": "module"
 */

// --- Core Dependencies ---
import express from 'express';
import cron from 'node-cron';
import { exec } from 'child_process';
import { promisify } from 'util';
import dotenv from 'dotenv';
dotenv.config();
import mysql from "mysql2/promise";

// --- Application Utilities & Middleware ---

const pool = mysql.createPool({
  host: process.env.MYSQL_HOST || "database-1.cxyc4mq4ohwl.us-east-1.rds.amazonaws.com",
  port: process.env.MYSQL_PORT ? parseInt(process.env.MYSQL_PORT, 10) : 3306,
  user: process.env.MYSQL_USER || "stacknow_user",
  password: process.env.MYSQL_PASSWORD || "stacknow_password", // Set your password in .env
  database: process.env.MYSQL_DATABASE || "kube_stats_db",      // Ensure this database exists
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

const getDB = () => pool;

// Promisify exec so we can use async/await
const execAsync = promisify(exec);

// --- In-Memory Cache & Configuration ---
const prevMetrics = {}; // Key: "<namespace>:<pod_name>"
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

// --- Bandwidth Metrics Collection Function ---
async function collectMetrics() {
  console.log(`[${new Date().toISOString()}] Starting bandwidth metrics collection...`);
  try {
    // Get all pods across all namespaces using kubectl and JSONPath
    const { stdout: podsOutput } = await execAsync(
      `kubectl get pods --all-namespaces -o jsonpath="{range .items[*]}{.metadata.namespace} {.metadata.name}{'\\n'}{end}"`
    );
    const podLines = podsOutput.trim().split('\n');
    for (const line of podLines) {
      // Split the line into namespace and pod name
      const parts = line.trim().split(/\s+/);
      if (parts.length < 2) continue;
      const namespace = parts[0];
      const pod = parts[1];

      // Skip processing if the namespace is in the excluded list.
      if (excludedNamespaces.has(namespace)) {
        continue;
      }

      try {
        // Fetch metrics from the istio-proxy container on the pod
        const { stdout: metrics } = await execAsync(
          `kubectl exec -n ${namespace} ${pod} -c istio-proxy -- curl -s http://localhost:15000/stats`
        );
        // Extract the rx and tx cumulative byte counts.
        const rxMatch = metrics.match(/upstream_cx_rx_bytes_total:\s*(\d+)/);
        const txMatch = metrics.match(/upstream_cx_tx_bytes_total:\s*(\d+)/);
        const rx = rxMatch ? parseInt(rxMatch[1], 10) : 0;
        const tx = txMatch ? parseInt(txMatch[1], 10) : 0;

        // Retrieve previous metrics (or assume zero if not found)
        const key = `${namespace}:${pod}`;
        const prev = prevMetrics[key] || { rx: 0, tx: 0 };

        // Calculate deltas. If values reset (e.g. pod restart) then use current value.
        let deltaRx = rx - prev.rx;
        let deltaTx = tx - prev.tx;
        if (deltaRx < 0) deltaRx = rx;
        if (deltaTx < 0) deltaTx = tx;

        // Update the cache with the current values
        prevMetrics[key] = { rx, tx };

        // Insert the metrics into the MySQL table.
        const db = getDB();
        await db.execute(
          `INSERT INTO bandwidth_metrics 
             (timestamp, namespace, pod_name, incoming_bandwidth, outgoing_bandwidth, incoming_delta, outgoing_delta)
           VALUES (NOW(), ?, ?, ?, ?, ?, ?)`,
          [namespace, pod, rx, tx, deltaRx, deltaTx]
        );
        console.log(`Metrics recorded for ${namespace}/${pod} => RX: ${rx} (Δ: ${deltaRx}), TX: ${tx} (Δ: ${deltaTx})`);
      } catch (podErr) {
        console.error(`Error processing pod ${namespace}/${pod}:`, podErr);
      }
    }
  } catch (err) {
    console.error('Error fetching pod list:', err);
  }
}

// --------------------------------------------------------------
// Cron Scheduling for Metrics Collection (EVERY 10 MINUTES)
// --------------------------------------------------------------
cron.schedule('0 */10 * * * *', () => {
  collectMetrics();
});

// --------------------------------------------------------------
// Create Express App and API Endpoint
// --------------------------------------------------------------
const app = express();
app.use(express.json());

// --------------------------------------------------------------
// Start the Server
// --------------------------------------------------------------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Bandwidth Metrics Service is running on port ${PORT}`);
});
