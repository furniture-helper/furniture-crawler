import { Pool, PoolClient } from 'pg';
import { Signer } from '@aws-sdk/rds-signer';
import logger from '../Logger';

// These credentials are only used for local development and testing.
const DEFAULT_PG_HOST = 'localhost';
const DEFAULT_PG_PORT = 5432;
const DEFAULT_PG_USER = 'furniture_crawler';
const DEFAULT_PG_PASSWORD = 'Test@123';
const DEFAULT_PG_DATABASE = 'furniture_crawler';

let pool: Pool | undefined;

async function createPool(): Promise<Pool> {
    const host = process.env.PG_HOST || DEFAULT_PG_HOST;
    const port = Number(process.env.PG_PORT || DEFAULT_PG_PORT);
    const username = process.env.RDS_IAM_USER || DEFAULT_PG_USER;
    const shouldUseRdsIamToken = !!process.env.RDS_IAM_USER || host !== DEFAULT_PG_HOST;
    const password =
        process.env.PG_PASSWORD ||
        (shouldUseRdsIamToken ? await getRdsIamToken(host, port, username) : DEFAULT_PG_PASSWORD);

    return new Pool({
        host,
        port,
        user: username,
        password,
        database: process.env.PG_DATABASE || DEFAULT_PG_DATABASE,
        max: 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 30000,
        ssl: process.env.DISABLE_DB_SSL == 'true' ? false : { rejectUnauthorized: false },
    });
}

export async function getPgClient(): Promise<PoolClient> {
    if (!pool) {
        pool = await createPool();
    }

    return await pool.connect();
}

async function getRdsIamToken(host: string, port: number, username: string): Promise<string> {
    logger.info(`Generating RDS IAM token for user ${username} at ${host}:${port}`);
    const signer = new Signer({
        region: process.env.AWS_REGION || 'eu-west-1',
        hostname: host,
        port,
        username,
    });
    return await signer.getAuthToken();
}
