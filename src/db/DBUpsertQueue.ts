import { getDomainFromUrl } from '../utils/url_utils';
import { getPgClient } from './pgClient';
import logger from '../Logger';
import { isBlacklistedUrl } from '../utils/crawler_utils';

export default class DatabaseUpsertQueue {
    private static totalUpserted = 0;
    private static checkedUrls: Set<string> = new Set<string>();

    public static async enqueueUpsert(url: string, s3Key: string): Promise<void> {
        if (isBlacklistedUrl(url)) {
            logger.info(`URL ${url} is blacklisted. Skipping upsert.`);
            return;
        }

        const domain = getDomainFromUrl(url);
        logger.debug(`Domain extracted: ${domain} from URL: ${url}`);

        const query = `
            INSERT INTO pages (url, domain, s3_key, is_active)
            VALUES ($1, $2, $3, true) ON CONFLICT (url) DO
            UPDATE SET domain = EXCLUDED.domain,
                s3_key = EXCLUDED.s3_key,
                is_active = true;
        `;
        const values = [url, domain, s3Key];

        logger.debug(`Attempting to get db connection for URL: ${url}`);
        const dbClient = await getPgClient();
        logger.debug(`DB connection acquired. Executing upsert for URL: ${url}`);

        try {
            logger.debug(`Executing upsert query for URL: ${url}`);
            await dbClient.query(query, values);
            DatabaseUpsertQueue.totalUpserted += 1;
            logger.info(`Upserted URL ${url} into database. Total upserted: ${DatabaseUpsertQueue.totalUpserted}`);
        } catch (err) {
            logger.error(err, `Error upserting URL ${url} into database.`);
            throw err;
        } finally {
            logger.debug(`Releasing db connection for URL: ${url}`);
            dbClient.release();
        }
    }

    public static async checkAndInsertNewUrl(url: string): Promise<void> {
        if (DatabaseUpsertQueue.checkedUrls.has(url)) {
            return;
        }
        DatabaseUpsertQueue.checkedUrls.add(url);

        if (isBlacklistedUrl(url)) {
            logger.info(`URL ${url} is blacklisted. Skipping insertion.`);
            return;
        }

        const domain = getDomainFromUrl(url);
        const query = `
            INSERT INTO pages (url, domain, s3_key, updated_at, is_active)
            VALUES ($1, $2, 'NOT_CRAWLED', to_timestamp(0), true) ON CONFLICT (url) DO NOTHING
            RETURNING url;
        `;
        const values = [url, domain];

        const dbClient = await getPgClient();
        try {
            const res = await dbClient.query(query, values);
            const inserted = (res.rowCount ?? 0) > 0;
            if (inserted) {
                logger.info(`Inserted new URL ${url} into database.`);
            }
        } catch (err) {
            DatabaseUpsertQueue.checkedUrls.delete(url);
            logger.error(err, `Error inserting URL ${url} into database.`);
            throw err;
        } finally {
            dbClient.release();
        }
    }

    public static async removeFromDatabase(url: string): Promise<void> {
        this.checkedUrls.add(url);
        const query = `
            UPDATE pages
            SET is_active = false
            WHERE url = $1;
        `;
        const values = [url];

        const dbClient = await getPgClient();
        try {
            await dbClient.query(query, values);
            logger.info(`Removed URL ${url} from database.`);
        } catch (err) {
            this.checkedUrls.delete(url);
            logger.error(err, `Error removing URL ${url} from database.`);
            throw err;
        } finally {
            dbClient.release();
        }
    }
}
