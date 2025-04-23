module.exports = {
  source: "api", // 'mongodb', 'api'
  target: "csv", // 'mysql', 'csv', 'postgresql'

  // For extracting from MongoDB
  mongoURI: "mongodb://localhost:27017/mform",
  mongoDBName: "mform",

  // For extracting from REST API
  apiURL: "https://dummyjson.com/users",
  apiToken: "API_KEY",

  // For loading to PostgreSQL
  postgresConfig: {
    user: "postgres",
    host: "localhost",
    database: "analytics",
    password: "password",
    port: 5432,
  },

  // For loading to MySQL
  mysqlConfig: {
    host: "localhost",
    user: "root",
    password: "password",
    database: "analytics",
  },

  // Used for automated incremental sync
  lastSyncedTimePath: "./lastSynced.json",
};
