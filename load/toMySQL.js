const mysql = require('mysql2/promise');
const config = require('../config/etl.config');

async function upsertToMySQL(masterRecords, childRecordsMap) {
  const conn = await mysql.createConnection(config.mysqlConfig);
  try {
    for (const master of masterRecords) {
      const keys = Object.keys(master);
      const values = keys.map(k => master[k]);
      const placeholders = keys.map(() => '?');

      const updateSet = keys.map(k => `${k} = VALUES(${k})`).join(', ');
      const sql = `INSERT INTO submissions (${keys.join(', ')}) VALUES (${placeholders.join(', ')})
                   ON DUPLICATE KEY UPDATE ${updateSet}`;

      await conn.execute(sql, values);
    }

    for (const [childKey, children] of Object.entries(childRecordsMap)) {
      for (const child of children) {
        const keys = Object.keys(child);
        const values = keys.map(k => child[k]);
        const placeholders = keys.map(() => '?');

        const updateSet = keys.map(k => `${k} = VALUES(${k})`).join(', ');
        const sql = `INSERT INTO ${childKey} (${keys.join(', ')}) VALUES (${placeholders.join(', ')})
                     ON DUPLICATE KEY UPDATE ${updateSet}`;

        await conn.execute(sql, values);
      }
    }
  } finally {
    await conn.end();
  }
}

module.exports = upsertToMySQL;
