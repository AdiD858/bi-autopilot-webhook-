import { Client as PgClient } from 'pg';

const API_KEY = process.env.QUERY_API_KEY;

async function runSql(sql, params = []) {
  const pg = new PgClient({
    connectionString: process.env.PG_CONNECTION,
    ssl: { rejectUnauthorized: false }
  });
  await pg.connect();
  try {
    const result = await pg.query(sql, params);
    return result.rows;
  } finally {
    await pg.end();
  }
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const authHeader = req.headers['x-api-key'];
  if (!API_KEY || authHeader !== API_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { sql, params } = req.body;

  if (!sql || typeof sql !== 'string') {
    return res.status(400).json({ error: 'Missing or invalid sql field' });
  }

  const upperSql = sql.trim().toUpperCase();
  if (!upperSql.startsWith('SELECT') && !upperSql.startsWith('WITH')) {
    return res.status(403).json({ error: 'Only SELECT queries are allowed' });
  }

  try {
    const rows = await runSql(sql, params || []);
    return res.status(200).json({ rows });
  } catch (err) {
    console.error('Query error:', err.message);
    return res.status(500).json({ error: err.message });
  }
}
