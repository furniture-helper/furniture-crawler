import { getDomainFromUrl } from '../utils/url_utils';
import { getPgClient } from './pgClient';
import logger from '../Logger';

type PageRow = {
    url: string;
    domain: string;
    s3_key: string;
};

export default class DatabaseUpsertQueue {
    private static rows: PageRow[] = [];
    private static processing = false;
    private static readonly CHUNK_SIZE: number = Number.parseInt(process.env.DB_UPSERT_CHUNK_SIZE ?? '100', 10) || 100;
    private static readonly MAX_QUEUE_SIZE: number =
        Number.parseInt(process.env.DB_UPSERT_MAX_QUEUE_SIZE ?? '1000', 10) || 1000;

    public static enqueueUpsert(url: string, s3Key: string): void {
        const domain = getDomainFromUrl(url);
        DatabaseUpsertQueue.rows.push({
            url: url,
            domain: domain,
            s3_key: s3Key,
        });
        logger.debug(`DatabaseUpsert enqueued with url ${url}`);

        if (DatabaseUpsertQueue.rows.length >= DatabaseUpsertQueue.MAX_QUEUE_SIZE) {
            logger.debug(
                `DatabaseUpsertQueue reached max size of ${DatabaseUpsertQueue.MAX_QUEUE_SIZE}. Processing queue.`,
            );
            if (!DatabaseUpsertQueue.processing) DatabaseUpsertQueue.processQueue().catch((err) => logger.error(err));
        }
    }

    private static async processQueue(): Promise<void> {
        if (DatabaseUpsertQueue.processing) return;
        DatabaseUpsertQueue.processing = true;
        try {
            while (DatabaseUpsertQueue.rows.length > 0) {
                const chunk = DatabaseUpsertQueue.rows.slice(0, DatabaseUpsertQueue.CHUNK_SIZE);
                if (chunk.length === 0) break;

                const values = chunk.flatMap((r) => [r.url, r.domain, r.s3_key, true]);
                const placeholders = chunk
                    .map((_, idx) => `($${idx * 4 + 1}, $${idx * 4 + 2}, $${idx * 4 + 3}, $${idx * 4 + 4})`)
                    .join(', ');

                const text = `
                    INSERT INTO pages (url, domain, s3_key, is_active)
                    VALUES ${placeholders}
                    ON CONFLICT (url) DO UPDATE
                    SET domain = EXCLUDED.domain,
                        s3_key = EXCLUDED.s3_key,
                        is_active = true;
                `;

                const dbClient = await getPgClient();
                try {
                    logger.debug(`Upserting ${chunk.length} rows into the database.`);
                    await dbClient.query('BEGIN');
                    await dbClient.query(text, values);
                    await dbClient.query('COMMIT');

                    DatabaseUpsertQueue.rows.splice(0, chunk.length);
                    logger.debug(`Successfully upserted ${chunk.length} rows into the database.`);
                } catch (err) {
                    await dbClient.query('ROLLBACK').catch((rollbackErr) => {
                        logger.error(rollbackErr, 'Error rolling back transaction after upsert failure.');
                    });
                    logger.error(err, 'Error upserting rows into the database.');
                    throw err;
                } finally {
                    dbClient.release();
                }
            }
        } finally {
            DatabaseUpsertQueue.processing = false;
        }
    }

    public static async flush(timeoutMs: number = 30000): Promise<void> {
        const start = Date.now();

        if (!DatabaseUpsertQueue.processing && DatabaseUpsertQueue.rows.length > 0) {
            logger.info('Flushing DatabaseUpsertQueue...');
            DatabaseUpsertQueue.processQueue().catch((err) => logger.error(err));
        }

        while (DatabaseUpsertQueue.processing || DatabaseUpsertQueue.rows.length > 0) {
            if (Date.now() - start >= timeoutMs) {
                throw new Error(
                    `Timeout after ${timeoutMs}ms waiting for DatabaseUpsertQueue.flush(). ` +
                        `Queue still has ${DatabaseUpsertQueue.rows.length} items pending; ` +
                        `processing=${DatabaseUpsertQueue.processing}.`,
                );
            }

            await new Promise((r) => setTimeout(r, 100));
        }
    }
}

const gracefulShutdown = async (signal: string) => {
    try {
        logger.info(`Received ${signal}, flushing DB upsert queue before exit...`);
        await DatabaseUpsertQueue.flush(30000); // 30s timeout
        logger.info('DB upsert queue flushed, exiting.');
        process.exit(0);
    } catch {
        logger.warn('Failed to flush DB upsert queue before exit (timeout or error). Exiting anyway.');
        process.exit(1);
    }
};

process.on('SIGINT', () => void gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => void gracefulShutdown('SIGTERM'));

process.on('beforeExit', () => {
    void DatabaseUpsertQueue.flush(5000).catch(() => {});
});
