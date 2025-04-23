// utils/logger.js
const fs = require("fs");
const path = require("path");

const logFile = path.join(__dirname, "../logs/etl.log");

function ensureLogDir() {
  const dir = path.dirname(logFile);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function logToFile(message) {
  ensureLogDir();
  const time = new Date().toISOString();
  fs.appendFileSync(logFile, `[${time}] ${message}\n`);
}

function logSuccess(recordId) {
  const message = `✅ Processed record ID: ${recordId}`;
  console.log(message);
  logToFile(message);
}

function logMessage(message) {
  console.log(message);
  logToFile(message);
}

function logDuplicate(recordId) {
  const message = `⚠️ Duplicate/skipped record ID: ${recordId}`;
  console.warn(message);
  logToFile(message);
}

function logError(recordId, error) {
  const message = `❌ Error processing ID ${recordId}: ${
    error.message || error
  }`;
  console.error(message);
  logToFile(message);
}

function logSummary(summary) {
  const message = `🧾 Summary:\n  ✔️ Success: ${summary.success}\n  ⚠️ Duplicates: ${summary.duplicates}\n  ❌ Failed: ${summary.failed}`;
  console.log(message);
  logToFile(message);
}

module.exports = {
  logSuccess,
  logDuplicate,
  logError,
  logSummary,
  logMessage,
};
