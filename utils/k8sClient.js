// utils/k8sClient.js
import {
  KubeConfig,
  CoreV1Api,
  AppsV1Api,
  NetworkingV1Api,
  PolicyV1Api,
  CustomObjectsApi, // <--- IMPORT THIS
} from "@kubernetes/client-node";
import { getSecretByName } from "./secretManager.js"; // Assuming this is in the same utils directory

const kcInstance = new KubeConfig(); // Renamed to avoid conflict with exported kc

// Variables to hold the initialized API clients
let coreV1Api, appsV1Api, networkingV1Api, policyV1Api, customObjectsApi, kc;

export const initKubernetesClients = async () => {
  try {
    const secretName = process.env.KUBECONFIG_SECRET_NAME;
    if (!secretName) {
      console.error("[ERROR k8sClient] KUBECONFIG_SECRET_NAME environment variable is not set.");
      throw new Error("Kubeconfig secret name not configured.");
    }

    console.log(`[INFO k8sClient] Attempting to load Kubeconfig from secret: ${secretName}`);
    const secret = await getSecretByName(secretName);

    if (!secret || !secret.value) {
      console.warn(`[WARN k8sClient] Kubernetes secret '${secretName}' not found or has no value. Skipping Kubernetes client initialization.`);
      // Set APIs to null or undefined to indicate failure
      coreV1Api = null;
      appsV1Api = null;
      networkingV1Api = null;
      policyV1Api = null;
      customObjectsApi = null;
      kc = null;
      return false; // Indicate initialization failure
    }

    kcInstance.loadFromString(secret.value);
    console.log("[INFO k8sClient] Kubeconfig loaded successfully from secret.");

    coreV1Api = kcInstance.makeApiClient(CoreV1Api);
    appsV1Api = kcInstance.makeApiClient(AppsV1Api);
    networkingV1Api = kcInstance.makeApiClient(NetworkingV1Api);
    policyV1Api = kcInstance.makeApiClient(PolicyV1Api);
    customObjectsApi = kcInstance.makeApiClient(CustomObjectsApi); // <--- INITIALIZE THIS
    kc = kcInstance; // Assign the configured KubeConfig instance to the exported variable

    console.log("[INFO k8sClient] Kubernetes API clients initialized.");
    return true; // Indicate initialization success
  } catch (error) {
    console.error("[ERROR k8sClient] Failed to initialize Kubernetes clients:", error.message);
    if (error.stack) {
        console.error(error.stack);
    }
    // Ensure APIs are null on failure to prevent partial initialization issues
    coreV1Api = null;
    appsV1Api = null;
    networkingV1Api = null;
    policyV1Api = null;
    customObjectsApi = null;
    kc = null;
    return false; // Indicate initialization failure
  }
};

// This function is an alternative way to get clients if you prefer,
// but podstats.js currently relies on initKubernetesClients and direct exports.
export const getKubernetesClients = async () => {
  const secretName = process.env.KUBECONFIG_SECRET_NAME;
  if (!secretName) {
    console.error("[ERROR k8sClient] KUBECONFIG_SECRET_NAME environment variable is not set for getKubernetesClients.");
    return null;
  }
  const secret = await getSecretByName(secretName);
  if (!secret || !secret.value) {
    console.warn(`[WARN k8sClient] Kubernetes secret '${secretName}' not found or has no value for getKubernetesClients. Skipping.`);
    return null;
  }

  const tempKc = new KubeConfig(); // Use a temporary KubeConfig instance for this function
  tempKc.loadFromString(secret.value);

  return {
    coreV1Api: tempKc.makeApiClient(CoreV1Api),
    appsV1Api: tempKc.makeApiClient(AppsV1Api),
    networkingV1Api: tempKc.makeApiClient(NetworkingV1Api),
    policyV1Api: tempKc.makeApiClient(PolicyV1Api),
    customObjectsApi: tempKc.makeApiClient(CustomObjectsApi), // <--- INCLUDE THIS
    kc: tempKc,
  };
};

// Export the initialized instances.
// podstats.js will import these directly after calling initKubernetesClients.
export { coreV1Api, appsV1Api, networkingV1Api, policyV1Api, customObjectsApi, kc };