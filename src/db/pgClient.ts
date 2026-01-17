import { Pool, PoolClient } from 'pg';

// These credentials are only used for local development and testing.
const DEFAULT_PG_HOST = 'localhost';
const DEFAULT_PG_PORT = 5432;
const DEFAULT_PG_USER = 'furniture_crawler';
const DEFAULT_PG_PASSWORD = 'Test@123';
const DEFAULT_PG_DATABASE = 'furniture_crawler';

const pool = new Pool({
    host: process.env.PG_HOST || DEFAULT_PG_HOST,
    port: Number(process.env.PG_PORT || DEFAULT_PG_PORT),
    user: process.env.PG_USER || DEFAULT_PG_USER,
    password: process.env.PG_PASSWORD || DEFAULT_PG_PASSWORD,
    database: process.env.PG_DATABASE || DEFAULT_PG_DATABASE,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 30000,
    ssl: process.env.DISABLE_DB_SSL == 'true' ? false : { rejectUnauthorized: false },
});

export async function getPgClient(): Promise<PoolClient> {
    return await pool.connect();
}
