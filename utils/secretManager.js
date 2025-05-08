// utils/secretManager.js
import crypto from "crypto";
import { mainDb, metricsDb } from "./db.js"; // Assuming db.js is in the same directory

// Corrected function declarations
const getmainDb = () => mainDb;       // Removed () after getmainDb
const getmetricsDB = () => metricsDb;   // Removed () after getmetricsDB


// --- Secure Encryption Key Retrieval ---
async function getEncryptionKey() {
  const key = process.env.SECRET_ENCRYPTION_KEY;
  if (!key) {
    console.error("[ERROR getEncryptionKey] SECRET_ENCRYPTION_KEY environment variable is not set.");
    throw new Error("Encryption configuration error: Key not set.");
  }
  // aes-256-cbc requires a 32-byte key
  if (Buffer.from(key, "utf8").length !== 32) {
    console.error(`[ERROR getEncryptionKey] Encryption key must be exactly 32 bytes (UTF-8 encoded). Current length: ${Buffer.from(key, "utf8").length} bytes.`);
    throw new Error("Encryption configuration error: Invalid key length.");
  }
  return Buffer.from(key, "utf8");
}

// --- Encrypt and Decrypt Helpers ---

async function decryptValue(encryptedValue, key) {
  if (typeof encryptedValue !== 'string' || !encryptedValue.includes(':')) {
    console.error(`[ERROR decryptValue] Invalid encrypted value format: ${encryptedValue}`);
    throw new Error("Invalid encrypted format. Expected 'iv:ciphertext'.");
  }
  const parts = encryptedValue.split(":");
  if (parts.length !== 2) {
    console.error(`[ERROR decryptValue] Invalid encrypted value format, expected 2 parts after split by ':' got ${parts.length}`);
    throw new Error("Invalid encrypted format. Expected 'iv:ciphertext'.");
  }
  const [ivB64, encryptedText] = parts;

  if (!ivB64 || !encryptedText) {
    console.error("[ERROR decryptValue] IV or encrypted text is missing after split.");
    throw new Error("Invalid encrypted format: IV or ciphertext missing.");
  }

  const iv = Buffer.from(ivB64, "base64");
  // For aes-256-cbc, IV should be 16 bytes
  if (iv.length !== 16) {
      console.error(`[ERROR decryptValue] Invalid IV length. Expected 16 bytes, got ${iv.length}.`);
      throw new Error("Decryption error: Invalid IV length.");
  }

  try {
    const decipher = crypto.createDecipheriv("aes-256-cbc", key, iv);
    let decrypted = decipher.update(encryptedText, "base64", "utf8");
    decrypted += decipher.final("utf8");
    return decrypted;
  } catch (decryptionError) {
    console.error("[ERROR decryptValue] Decryption failed:", decryptionError.message);
    throw new Error("Failed to decrypt value. Check encryption key or data integrity.");
  }
}


// Get secret by name (specific version or latest)
const getSecret = async (secretName, userId, userUUID, version = 'latest') => {
  console.log(`[DEBUG getSecret] Attempting to get secret: name='${secretName}', userId=${userId}, userUUID='${userUUID}', version='${version}'`);
  if (!secretName || userId === undefined || userId === null || !userUUID) { // Added check for userId existence
    console.error("[ERROR getSecret] Missing fields for secret retrieval (secretName, userId, userUUID).");
    throw new Error("Missing fields for secret retrieval (secretName, userId, userUUID).");
  }

  let encryptionKey;
  try {
    encryptionKey = await getEncryptionKey();
  } catch (keyError) {
    // Error already logged in getEncryptionKey
    throw keyError; // Re-throw to stop execution
  }

  const trimmedName = String(secretName).trim(); // Ensure secretName is a string before trim
  console.log(`[DEBUG getSecret] Trimmed name: '${trimmedName}'`);

  let query, params;

  if (version === 'latest') {
    console.log(`[DEBUG getSecret] Setting query for 'latest' version.`);
    query = `
      SELECT sv.secret_value, sv.secret_version
      FROM secret_metadata sm
      JOIN secret_versions sv ON sm.id = sv.secret_metadata_id
      WHERE sm.secret_name = ? AND sm.user_id = ? AND sm.user_uuid = ?
      ORDER BY sv.secret_version DESC LIMIT 1
    `;
    params = [trimmedName, userId, userUUID];
  } else {
    const versionInt = parseInt(version, 10);
    if (isNaN(versionInt) || versionInt <= 0) {
         console.error(`[ERROR getSecret] Invalid non-numeric or non-positive version specified: ${version}`);
         throw new Error(`Invalid version specified: ${version}. Must be 'latest' or a positive number.`);
    }
    console.log(`[DEBUG getSecret] Setting query for specific version: ${versionInt}.`);
    query = `
      SELECT sv.secret_value, sv.secret_version
      FROM secret_metadata sm
      JOIN secret_versions sv ON sm.id = sv.secret_metadata_id
      WHERE sm.secret_name = ? AND sm.user_id = ? AND sm.user_uuid = ? AND sv.secret_version = ?
      LIMIT 1
    `;
    params = [trimmedName, userId, userUUID, versionInt];
  }

  // This check should ideally not be needed if logic above is sound.
  if (!query || !params) {
     console.error(`[CRITICAL getSecret] Query or Params were not defined before DB call. Name='${trimmedName}', userId=${userId}, userUUID='${userUUID}', version='${version}'`);
     throw new Error("Internal error: Failed to construct database query for secret retrieval.");
  }

  try {
    console.log(`[DEBUG getSecret] Executing query with params: ${JSON.stringify(params)}`);
    const [rows] = await getmainDb().query(query, params);
    console.log(`[DEBUG getSecret] DB query returned ${rows.length} row(s).`);

    if (!rows || rows.length === 0) { // Added check for !rows
      const errorMessage = `Secret '${trimmedName}' (version: ${version}) not found for user ID ${userId}, UUID ${userUUID}.`;
      console.warn(`[WARN getSecret] ${errorMessage}`); // Changed to WARN as 404 is a common case
      const notFoundError = new Error(errorMessage);
      notFoundError.status = 404; // Standard HTTP status code for Not Found
      throw notFoundError;
    }

    const encryptedValue = rows[0].secret_value;
    const retrievedVersion = rows[0].secret_version;
    console.log(`[DEBUG getSecret] Found encrypted value for version ${retrievedVersion}. Attempting decryption...`);

    const decrypted = await decryptValue(encryptedValue, encryptionKey);
    console.log(`[INFO getSecret] Decryption successful for secret '${trimmedName}', version ${retrievedVersion}.`);

    return { secretName: trimmedName, value: decrypted, version: retrievedVersion };

  } catch (dbOrDecryptError) {
    // Log detailed error but return a more generic message to the caller unless it's a 404
    console.error(`[ERROR getSecret] Error during DB query or decryption for name='${trimmedName}', userId=${userId}, userUUID='${userUUID}', version='${version}':`, dbOrDecryptError.message, dbOrDecryptError.stack);
    if (dbOrDecryptError.status === 404) throw dbOrDecryptError; // Re-throw specific 404
    // For other errors, throw a generic error to avoid leaking sensitive details
    throw new Error(`Failed to retrieve or decrypt secret '${trimmedName}'. Please check logs for details.`);
  }
};


const getSecretByName = async (secretName, version = 'latest') => {
  console.log(`[DEBUG getSecretByName] Attempting to get secret: name='${secretName}', version='${version}'`);
  if (!secretName) {
    console.error("[ERROR getSecretByName] Missing secretName for secret retrieval.");
    throw new Error("Missing secretName for secret retrieval.");
  }

  let encryptionKey;
  try {
    encryptionKey = await getEncryptionKey();
  } catch (keyError) {
    throw keyError;
  }

  const trimmedName = String(secretName).trim();
  console.log(`[DEBUG getSecretByName] Trimmed name: '${trimmedName}'`);

  let query, params;

  if (version === 'latest') {
    console.log(`[DEBUG getSecretByName] Setting query for 'latest' version.`);
    query = `
      SELECT sv.secret_value, sv.secret_version, sm.user_id, sm.user_uuid 
      FROM secret_metadata sm
      JOIN secret_versions sv ON sm.id = sv.secret_metadata_id
      WHERE sm.secret_name = ?
      ORDER BY sv.secret_version DESC LIMIT 1
    `; // Added user_id, user_uuid for context if needed, though not used in return
    params = [trimmedName];
  } else {
    const versionInt = parseInt(version, 10);
    if (isNaN(versionInt) || versionInt <= 0) {
      console.error(`[ERROR getSecretByName] Invalid non-numeric or non-positive version specified: ${version}`);
      throw new Error(`Invalid version: ${version}. Must be 'latest' or a positive number.`);
    }
    console.log(`[DEBUG getSecretByName] Setting query for specific version: ${versionInt}.`);
    query = `
      SELECT sv.secret_value, sv.secret_version, sm.user_id, sm.user_uuid
      FROM secret_metadata sm
      JOIN secret_versions sv ON sm.id = sv.secret_metadata_id
      WHERE sm.secret_name = ? AND sv.secret_version = ?
      LIMIT 1
    `; // Added user_id, user_uuid
    params = [trimmedName, versionInt];
  }

  try {
    console.log(`[DEBUG getSecretByName] Executing query with params: ${JSON.stringify(params)}`);
    const [rows] = await getmainDb().query(query, params);
    console.log(`[DEBUG getSecretByName] DB query returned ${rows.length} row(s).`);

    if (!rows || rows.length === 0) {
      const errorMessage = `Secret '${trimmedName}' (version: ${version}) not found.`;
      console.warn(`[WARN getSecretByName] ${errorMessage}`);
      const err = new Error(errorMessage);
      err.status = 404;
      throw err;
    }

    const encryptedValue = rows[0].secret_value;
    const retrievedVersion = rows[0].secret_version;
    // const userId = rows[0].user_id; // Available if needed
    // const userUUID = rows[0].user_uuid; // Available if needed
    console.log(`[DEBUG getSecretByName] Found encrypted value for version ${retrievedVersion}. Attempting decryption...`);

    const decrypted = await decryptValue(encryptedValue, encryptionKey);
    console.log(`[INFO getSecretByName] Decryption successful for secret '${trimmedName}', version ${retrievedVersion}.`);

    return { secretName: trimmedName, value: decrypted, version: retrievedVersion };
  } catch (err) {
    console.error(`[ERROR getSecretByName] Error during DB query or decryption for name='${trimmedName}', version='${version}':`, err.message, err.stack);
    if (err.status === 404) throw err;
    throw new Error(`Failed to retrieve or decrypt secret '${trimmedName}'. Please check logs for details.`);
  }
};

// --- Exported Functions ---
export { getSecret, getSecretByName, getEncryptionKey, decryptValue }; // Exported helpers for potential direct use/testing