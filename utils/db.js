// utils/db.js
import mysql from "mysql2/promise";
import dotenv from "dotenv";

dotenv.config(); // Load environment variables

// Configuration with some best-practice defaults for pools
const dbConfigDefaults = {
  waitForConnections: true,
  connectionLimit: 10, // Adjust as needed
  queueLimit: 0, // Unlimited queue
  connectTimeout: 10000, // 10 seconds
};

const mainDb = mysql.createPool({
  ...dbConfigDefaults,
  host: process.env.MAINDB_HOST,
  user: process.env.MAINDB_USER,
  password: process.env.MAINDB_PASSWORD,
  database: process.env.MAINDB_NAME,
});

const metricsDb = mysql.createPool({
  ...dbConfigDefaults,
  host: process.env.METRICSDB_HOST,
  user: process.env.METRICSDB_USER,
  password: process.env.METRICSDB_PASSWORD,
  database: process.env.METRICSDB_NAME,
});

// Optional: Test connections on startup
(async () => {
  try {
    const mainConn = await mainDb.getConnection();
    console.log("[INFO] Connected to Main DB successfully.");
    mainConn.release();
  } catch (err) {
    console.error("[ERROR] Main DB connection failed:", err.message);
  }

  try {
    const metricsConn = await metricsDb.getConnection();
    console.log("[INFO] Connected to Metrics DB successfully.");
    metricsConn.release();
  } catch (err) {
    console.error("[ERROR] Metrics DB connection failed:", err.message);
  }
})();

export { mainDb, metricsDb };