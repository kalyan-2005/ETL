const { Pool } = require('pg');
const config = require('../config/etl.config');

const pool = new Pool(config.postgresConfig);

async function upsertToPostgres(masterRecords, childRecordsMap) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (const master of masterRecords) {
      const keys = Object.keys(master);
      const values = keys.map(k => master[k]);
      const placeholders = keys.map((_, i) => `$${i + 1}`);

      const updateSet = keys.map((k, i) => `${k} = EXCLUDED.${k}`).join(', ');
      const sql = `INSERT INTO submissions (${keys.join(', ')}) VALUES (${placeholders.join(', ')})
                   ON CONFLICT (submission_id) DO UPDATE SET ${updateSet}`;

      await client.query(sql, values);
    }

    for (const [childKey, children] of Object.entries(childRecordsMap)) {
      for (const child of children) {
        const keys = Object.keys(child);
        const values = keys.map(k => child[k]);
        const placeholders = keys.map((_, i) => `$${i + 1}`);

        const updateSet = keys.map((k, i) => `${k} = EXCLUDED.${k}`).join(', ');
        const sql = `INSERT INTO ${childKey} (${keys.join(', ')}) VALUES (${placeholders.join(', ')})
                     ON CONFLICT (submission_id) DO UPDATE SET ${updateSet}`;
        await client.query(sql, values);
      }
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

module.exports = upsertToPostgres;
