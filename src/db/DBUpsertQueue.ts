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
    private static rowIndex = 0;
    private static totalUpserted = 0;

    public static async enqueueUpsert(url: string, s3Key: string): Promise<void> {
        const domain = getDomainFromUrl(url);

        // wait for a maximum of 15 seconds if the queue is being processed
        const start = Date.now();
        while (DatabaseUpsertQueue.processing) {
            if (Date.now() - start > 15000) {
                throw new Error('Timeout waiting for DatabaseUpsertQueue to be free for enqueueing.');
            }
            await new Promise((r) => setTimeout(r, 100));
        }

        DatabaseUpsertQueue.rows.push({
            url: url,
            domain: domain,
            s3_key: s3Key,
        });
        logger.debug(`DatabaseUpsert enqueued with url ${url}`);

        if (DatabaseUpsertQueue.rows.length >= DatabaseUpsertQueue.rowIndex + DatabaseUpsertQueue.MAX_QUEUE_SIZE) {
            logger.info(
                `DatabaseUpsertQueue reached max size of ${DatabaseUpsertQueue.MAX_QUEUE_SIZE}. Processing queue.`,
            );
            if (!DatabaseUpsertQueue.processing) {
                await DatabaseUpsertQueue.processQueue();
            }
        }
    }

    private static async processQueue(): Promise<void> {
        if (DatabaseUpsertQueue.processing) return;
        DatabaseUpsertQueue.processing = true;
        try {
            while (DatabaseUpsertQueue.rows.length > DatabaseUpsertQueue.rowIndex) {
                const chunk = DatabaseUpsertQueue.rows.slice(
                    DatabaseUpsertQueue.rowIndex,
                    DatabaseUpsertQueue.rowIndex + DatabaseUpsertQueue.CHUNK_SIZE,
                );
                if (chunk.length === 0) break;

                const uniqueMap = new Map<string, PageRow>();
                for (const r of chunk) uniqueMap.set(r.url, r);
                const deduped = Array.from(uniqueMap.values());


                if (deduped.length !== chunk.length) {
                    logger.debug(
                        `Deduplicated ${chunk.length - deduped.length} duplicate URLs in chunk before upsert.`,
                    );
                }

                const values = deduped.flatMap((r) => [r.url, r.domain, r.s3_key, true]);
                const placeholders = deduped
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
                    logger.debug(`Upserting ${deduped.length} unique rows into database.`);
                    await dbClient.query('BEGIN');
                    await dbClient.query(text, values);
                    await dbClient.query('COMMIT');

                    logger.info(`Successfully upserted ${deduped.length} rows into database.`);

                    DatabaseUpsertQueue.totalUpserted += deduped.length;
                    logger.info(`Total upserted count: ${DatabaseUpsertQueue.totalUpserted}`);

                    DatabaseUpsertQueue.rowIndex += chunk.length;
                } catch (err) {
                    await dbClient.query('ROLLBACK').catch(() => {});
                    logger.error(err, 'Error upserting rows into database.');
                    // do not remove rows so they can be retried
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

        if (!DatabaseUpsertQueue.processing && DatabaseUpsertQueue.rows.length > DatabaseUpsertQueue.rowIndex) {
            logger.info('Flushing DatabaseUpsertQueue...');
            DatabaseUpsertQueue.processQueue().catch((err) => logger.error(err));
        }

        while (DatabaseUpsertQueue.processing || DatabaseUpsertQueue.rows.length > DatabaseUpsertQueue.rowIndex) {
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

    public static get totalUpsertedCount(): number {
        return DatabaseUpsertQueue.totalUpserted;
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
