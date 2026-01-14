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

            // Process the queue if it exceeds the max size or at every 30-minute interval
            const isCurrentTimeA10MinuteInterval = Math.floor(Date.now() / 60000) % 30 === 0;
            if (
                DatabaseUpsertQueue.rows.length >= DatabaseUpsertQueue.MAX_QUEUE_SIZE &&
                isCurrentTimeA10MinuteInterval
            ) {
                logger.info(
                    `Database upsert queue size ${DatabaseUpsertQueue.rows.length} exceeded max limit of ${DatabaseUpsertQueue.MAX_QUEUE_SIZE}. Processing queue...`,
                );
                await DatabaseUpsertQueue.process();
            }
        });
    }

    private static async process(): Promise<void> {
        const startTime = Date.now();
        while (DatabaseUpsertQueue.rows.length > 0) {
            if (Date.now() - startTime > 30000) {
                logger.warn('Database upsert processing time exceeded 30 seconds, aborting to avoid long lock.');
                break;
            }

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

    public static async flush(): Promise<void> {
        await DatabaseUpsertQueue.rowsMutex.runExclusive(async () => {
            await DatabaseUpsertQueue.process();
        });
    }

    public static get totalUpsertedCount(): number {
        return DatabaseUpsertQueue.totalUpserted;
    }
}

const gracefulShutdown = async (signal: string) => {
    try {
        logger.info(`Received ${signal}, flushing DB upsert queue before exit...`);
        await DatabaseUpsertQueue.flush();

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
        await DatabaseUpsertQueue.flush();
    } finally {
        // reference the getter so it's not flagged as unused by static analysis
        logger.info(`Total pages upserted to DB: ${DatabaseUpsertQueue.totalUpsertedCount}`);
    }
});
