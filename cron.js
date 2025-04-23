const cron = require('node-cron');
const { exec } = require('child_process');

cron.schedule('*/15 * * * *', () => {
  console.log('Running ETL...');
  exec('node index.js');
});
