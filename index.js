const config = require('./config/etl.config');
const fs = require('fs');
const path = require('path');

const extractFromMongo = require('./extract/fromMongo');
const extractFromAPI = require('./extract/fromAPI');
const flatten = require('./transform/flatten');

const upsertToPostgres = require('./load/toPostgres');
const upsertToMySQL = require('./load/toMySQL');
const exportToCSV = require('./load/toCSV');

const {
  logSuccess,
  logDuplicate,
  logError,
  logSummary,
  logMessage,
} = require('./utils/logger');

(async () => {
  const lastSync = fs.existsSync(config.lastSyncedTimePath)
    ? JSON.parse(fs.readFileSync(config.lastSyncedTimePath)).lastSync
    : null;

  const startMsg = `🔄 Starting ETL | Source: ${config.source}, Target: ${config.target}`;
  logMessage(startMsg);

  let rawSubmissions = [];
  try {
    rawSubmissions =
      config.source === 'mongodb'
        ? await extractFromMongo(lastSync)
        : await extractFromAPI(lastSync);

    logMessage(`📥 Extracted ${rawSubmissions.length} raw submissions.`);
  } catch (err) {
    logError('Data Extraction', err);
    process.exit(1);
  }

  const masterRecords = [];
  const childRecordsMap = {};
  const summary = { success: 0, duplicates: 0, failed: 0 };

  for (let i = 0; i < rawSubmissions.length; i++) {
    try {
      const submission = rawSubmissions[i];
      const { master, children } = flatten(submission);
      masterRecords.push(master);

      for (const [key, val] of Object.entries(children)) {
        if (!childRecordsMap[key]) childRecordsMap[key] = [];
        childRecordsMap[key].push(...val);
      }

      logSuccess(master._id || `#${i + 1}`);
      summary.success++;
    } catch (err) {
      logError(`Flatten ID ${rawSubmissions[i]._id || `#${i + 1}`}`, err);
      summary.failed++;
    }
  }

  try {
    switch (config.target) {
      case 'postgresql':
        await upsertToPostgres(masterRecords, childRecordsMap);
        break;
      case 'mysql':
        await upsertToMySQL(masterRecords, childRecordsMap);
        break;
      case 'csv':
        await exportToCSV(masterRecords, childRecordsMap);
        break;
    }

    fs.writeFileSync(config.lastSyncedTimePath, JSON.stringify({ lastSync: new Date().toISOString() }));
    logMessage(`✅ ETL completed successfully. Processed ${masterRecords.length} records.`);
  } catch (err) {
    logError('Data Loading', err);
    summary.failed += masterRecords.length;
  }

  logSummary(summary);
})();