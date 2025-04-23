const { MongoClient } = require('mongodb');
const config = require('../config/etl.config');

async function extractFromMongo(lastSyncedTime) {
  const client = new MongoClient(config.mongoURI);
  await client.connect();
  const db = client.db(config.mongoDBName);

  const query = lastSyncedTime
    ? { updatedAt: { $gt: new Date(lastSyncedTime) } }
    : {};

  const submissions = await db.collection('submissions').find(query).toArray();
  await client.close();
  return submissions;
}

module.exports = extractFromMongo;
