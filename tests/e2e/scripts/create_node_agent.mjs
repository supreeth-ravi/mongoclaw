import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { MongoClawClient } from '../../../sdk-nodejs/dist/index.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const fixturePath = path.join(__dirname, '..', 'fixtures', 'method_node_agent.json');
const config = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));

const client = new MongoClawClient({ baseUrl: 'http://127.0.0.1:8000', apiKey: 'test-key' });
const created = await client.createAgent(config);
console.log(created.id);
