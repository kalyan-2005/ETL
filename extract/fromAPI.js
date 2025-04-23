const axios = require('axios');
const config = require('../config/etl.config');

async function extractFromAPI(lastSyncedTime) {
  const response = await axios.get(config.apiURL, {
    headers: { Authorization: `Bearer ${config.apiToken}` },
    params: lastSyncedTime ? { updatedAfter: lastSyncedTime } : {}
  });

  return response.data.users || [];
}

module.exports = extractFromAPI;
