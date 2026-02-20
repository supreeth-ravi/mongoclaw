// MongoDB Replica Set Initialization Script
// Required for change streams to work

// Wait for MongoDB to be ready
sleep(2000);

// Initialize replica set if not already done
try {
  const status = rs.status();
  print("Replica set already initialized");
  printjson(status);
} catch (e) {
  print("Initializing replica set...");

  const config = {
    _id: "rs0",
    members: [
      {
        _id: 0,
        host: "localhost:27017",
        priority: 1
      }
    ]
  };

  const result = rs.initiate(config);
  printjson(result);

  // Wait for replica set to be ready
  sleep(5000);

  // Create mongoclaw database and collections
  print("Creating MongoClaw database...");

  const db = db.getSiblingDB("mongoclaw");

  // Create agents collection with indexes
  db.createCollection("agents");
  db.agents.createIndex({ "id": 1 }, { unique: true });
  db.agents.createIndex({ "enabled": 1 });
  db.agents.createIndex({ "watch.database": 1, "watch.collection": 1 });

  // Create executions collection with indexes
  db.createCollection("executions");
  db.executions.createIndex({ "agent_id": 1, "created_at": -1 });
  db.executions.createIndex({ "status": 1 });
  db.executions.createIndex({ "document_id": 1 });
  db.executions.createIndex({ "created_at": 1 }, { expireAfterSeconds: 604800 }); // 7 days TTL

  // Create resume_tokens collection
  db.createCollection("resume_tokens");
  db.resume_tokens.createIndex({ "watcher_id": 1 }, { unique: true });

  // Create idempotency_keys collection with TTL
  db.createCollection("idempotency_keys");
  db.idempotency_keys.createIndex({ "key": 1 }, { unique: true });
  db.idempotency_keys.createIndex({ "created_at": 1 }, { expireAfterSeconds: 86400 }); // 24h TTL

  print("MongoClaw database initialized successfully");
}
