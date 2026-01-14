import { Pool, PoolClient } from 'pg';
import logger from '../Logger';

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
    ssl: false,
});

export async function getPgClient(): Promise<PoolClient> {
    return await pool.connect();
}

let isShuttingDown = false;

async function closePool(): Promise<void> {
    await pool.end();
}

async function gracefulShutdown(signal?: string, exitCode = 0): Promise<void> {
    if (isShuttingDown) return;
    isShuttingDown = true;
    try {
        // wait 15s
        await new Promise((resolve) => setTimeout(resolve, 15000));
        logger.info(`Received ${signal ?? 'exit event'}. Closing pg pool.`);
        await closePool();
    } catch (err) {
        logger.error(err, 'Error closing pg pool.');
        exitCode = exitCode || 1;
    } finally {
        if (typeof signal !== 'undefined') process.exit(exitCode);
    }
}

process.on('SIGINT', () => gracefulShutdown('SIGINT', 0));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM', 0));
process.on('beforeExit', () => gracefulShutdown(undefined, 0));
process.on('uncaughtException', () => {
    gracefulShutdown('uncaughtException', 1);
});
process.on('unhandledRejection', () => {
    gracefulShutdown('unhandledRejection', 1);
});
