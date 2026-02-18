import { getDomainFromUrl } from '../utils/url_utils';
import { getPgClient } from './pgClient';
import logger from '../Logger';
import { isBlacklistedUrl } from '../utils/crawler_utils';

export default class DatabaseUpsertQueue {
    private static totalUpserted = 0;
    private static checkedUrls: Set<string> = new Set<string>();

    public static async upsertPage(url: string, s3Key: string): Promise<void> {
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
            logger.debug(`URL ${url} is blacklisted. Skipping insertion.`);
            return;
        }

        const domain = getDomainFromUrl(url);
        const query = `
            INSERT INTO pages (url, domain, s3_key, updated_at, is_active)
            VALUES ($1, $2, 'NOT_CRAWLED', to_timestamp(0), true) ON CONFLICT (url) DO
            UPDATE
                SET is_active = true
                RETURNING url, (xmax = 0) AS inserted;
        `;
        const values = [url, domain];

        const dbClient = await getPgClient();
        try {
            const res = await dbClient.query(query, values);
            const wasInserted = res.rows[0]?.inserted === true;
            if (wasInserted) {
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

    public static async setInactive(url: string): Promise<void> {
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
            logger.info(`Set URL ${url} as inactive in database.`);
        } catch (err) {
            this.checkedUrls.delete(url);
            logger.error(err, `Error setting URL ${url} as inactive database.`);
            throw err;
        } finally {
            dbClient.release();
        }
    }

    public static async deleteFromDatabase(url: string): Promise<void> {
        this.checkedUrls.add(url);
        const query = `
            DELETE
            FROM pages
            WHERE url = $1;
        `;
        const values = [url];

        const dbClient = await getPgClient();
        try {
            await dbClient.query(query, values);
            logger.info(`Deleted URL ${url} from database.`);
        } catch (err) {
            this.checkedUrls.delete(url);
            logger.error(err, `Error deleting URL ${url} from database.`);
            throw err;
        } finally {
            dbClient.release();
        }
    }
}
