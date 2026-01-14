import { getDomainFromUrl } from '../utils/url_utils';
import { getPgClient } from './pgClient';
import logger from '../Logger';
import { Mutex } from 'async-mutex';

type PageRow = {
    url: string;
    domain: string;
    s3_key: string;
};

export default class DatabaseUpsertQueue {
    private static readonly CHUNK_SIZE: number = Number.parseInt(process.env.DB_UPSERT_CHUNK_SIZE ?? '100', 10) || 100;
    private static readonly MAX_QUEUE_SIZE: number =
        Number.parseInt(process.env.DB_UPSERT_MAX_QUEUE_SIZE ?? '1000', 10) || 1000;

    private static rows: PageRow[] = [];
    private static rowsMutex = new Mutex();
    private static totalUpserted = 0;

    public static async enqueueUpsert(url: string, s3Key: string): Promise<void> {
        await DatabaseUpsertQueue.rowsMutex.runExclusive(async () => {
            const domain = getDomainFromUrl(url);
            DatabaseUpsertQueue.rows.push({ url, domain, s3_key: s3Key });
            logger.debug(`DatabaseUpsert enqueued with url ${url}`);

            if (DatabaseUpsertQueue.rows.length >= DatabaseUpsertQueue.MAX_QUEUE_SIZE) {
                logger.info(
                    `Database upsert queue size ${DatabaseUpsertQueue.rows.length} exceeded max limit of ${DatabaseUpsertQueue.MAX_QUEUE_SIZE}. Processing queue...`,
                );
                await DatabaseUpsertQueue.process();
            }
        });
    }

    private static async process(): Promise<void> {
        while (DatabaseUpsertQueue.rows.length > 0) {
            const chunk = DatabaseUpsertQueue.rows.slice(0, DatabaseUpsertQueue.CHUNK_SIZE);
            if (chunk.length === 0) break;

            const uniqueMap = new Map<string, PageRow>();
            for (const r of chunk) uniqueMap.set(r.url, r);
            const deduped = Array.from(uniqueMap.values());

            if (deduped.length !== chunk.length) {
                logger.debug(`Deduplicated ${chunk.length - deduped.length} duplicate URLs in chunk before upsert.`);
            }

            const values = deduped.flatMap((r) => [r.url, r.domain, r.s3_key, true]);
            const placeholders = deduped
                .map((_, idx) => `($${idx * 4 + 1}, $${idx * 4 + 2}, $${idx * 4 + 3}, $${idx * 4 + 4})`)
                .join(', ');

            const text = `
                INSERT INTO pages (url, domain, s3_key, is_active)
                VALUES ${placeholders} ON CONFLICT (url) DO
                UPDATE
                    SET domain = EXCLUDED.domain,
                    s3_key = EXCLUDED.s3_key,
                    is_active = true;
            `;

            const dbClient = await getPgClient();
            try {
                logger.debug(`Upserting ${deduped.length} unique rows into database.`);
                await dbClient.query('BEGIN');
                await dbClient.query(text, values);
                await dbClient.query('COMMIT');

                DatabaseUpsertQueue.rows.splice(0, chunk.length);
                logger.info(`Successfully upserted ${deduped.length} rows into database.`);

                DatabaseUpsertQueue.totalUpserted += deduped.length;
                logger.info(`Total upserted count: ${DatabaseUpsertQueue.totalUpserted}`);
            } catch (err) {
                await dbClient.query('ROLLBACK').catch(() => {
                    logger.error('Failed to rollback transaction after upsert error.');
                });
                logger.error(err, 'Error upserting rows into database.');
                throw err;
            }
            dbClient.release();
        }
    }

    public static async flush(timeoutMs: number = 30000): Promise<void> {
        const start = Date.now();
        DatabaseUpsertQueue.rowsMutex.runExclusive(async () => {
            if (DatabaseUpsertQueue.rows.length === 0) {
                logger.info('Database upsert queue is empty, nothing to flush.');
                return;
            }

            logger.info('Flushing database upsert queue...');
            const flushPromise = DatabaseUpsertQueue.process();

            if (timeoutMs > 0) {
                const timeoutPromise = new Promise<void>((_, reject) => {
                    setTimeout(() => {
                        reject(new Error('Timeout while flushing database upsert queue.'));
                    }, timeoutMs);
                });

                await Promise.race([flushPromise, timeoutPromise]);
            } else {
                await flushPromise;
            }

            const duration = Date.now() - start;
            logger.info(`Database upsert queue flushed in ${duration} ms.`);
        });
    }

    public static get totalUpsertedCount(): number {
        return DatabaseUpsertQueue.totalUpserted;
    }
}

const gracefulShutdown = async (signal: string) => {
    try {
        logger.info(`Received ${signal}, flushing DB upsert queue before exit...`);
        // Force flush during graceful shutdown even if the queue hasn't exceeded the limit
        await DatabaseUpsertQueue.flush(30000); // 30s timeout
        logger.info('DB upsert queue flushed, exiting.');
        process.exit(0);
    } catch (error) {
        logger.warn({ error }, 'Failed to flush DB upsert queue before exit (timeout or error). Exiting anyway.');
        process.exit(1);
    }
};

process.on('SIGINT', () => void gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => void gracefulShutdown('SIGTERM'));

process.once('beforeExit', async () => {
    try {
        // don't force start here; only attempt to flush if the queue already exceeded the limit
        await DatabaseUpsertQueue.flush(5000);
    } finally {
        // reference the getter so it's not flagged as unused by static analysis
        logger.info(`Total pages upserted to DB: ${DatabaseUpsertQueue.totalUpsertedCount}`);
    }
});
