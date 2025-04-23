const fs = require('fs');
const { format } = require('@fast-csv/format');
const path = require('path');

async function exportToCSV(masterRecords, childRecordsMap) {
  const masterFile = fs.createWriteStream('output/master.csv');
  const masterCSV = format({ headers: true });
  masterCSV.pipe(masterFile);
  masterRecords.forEach(record => masterCSV.write(record));
  masterCSV.end();

  for (const [key, children] of Object.entries(childRecordsMap)) {
    const childFile = fs.createWriteStream(`output/${key}.csv`);
    const childCSV = format({ headers: true });
    childCSV.pipe(childFile);
    children.forEach(record => childCSV.write(record));
    childCSV.end();
  }
}

module.exports = exportToCSV;
