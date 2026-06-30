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
            INSERT INTO pages (url, domain, s3_key, is_active, last_crawled_at)
            VALUES ($1, $2, $3, true, $4) ON CONFLICT (url) DO
            UPDATE SET domain = EXCLUDED.domain,
                s3_key = EXCLUDED.s3_key,
                is_active = true,
                last_crawled_at = $4
        `;
        const values = [url, domain, s3Key, new Date()];

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

    public static async markAsCrawled(url: string): Promise<void> {
        const domain = getDomainFromUrl(url);
        logger.debug(`Domain extracted: ${domain} from URL: ${url}`);

        const query = `
            INSERT INTO pages (url, domain, s3_key, last_crawled_at)
            VALUES ($1, $2, 'REDIRECT', $3) ON CONFLICT (url) DO
            UPDATE SET domain = EXCLUDED.domain,
                s3_key = 'REDIRECT',
                last_crawled_at = $3
        `;
        const values = [url, domain, new Date()];

        logger.debug(`Attempting to get db connection for URL: ${url}`);
        const dbClient = await getPgClient();
        logger.debug(`DB connection acquired. Executing mark as crawled for URL: ${url}`);

        try {
            logger.debug(`Executing mark as crawled query for URL: ${url}`);
            await dbClient.query(query, values);
            logger.info(`Marked as crawled URL ${url} into database.`);
        } catch (err) {
            logger.error(err, `Error marking as crawl URL ${url} into database.`);
            throw err;
        } finally {
            logger.debug(`Releasing db connection for URL: ${url}`);
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

    public static async markAsProductPage(url: string, s3_key: string): Promise<void> {
        const query = `
            INSERT INTO page_classifications (url, s3_key, type, last_classified_at, updated_at)
            VALUES ($1, $2, 'product', $3, $3) ON CONFLICT (url) DO
            UPDATE SET type = 'product',
                s3_key = $2,
                last_classified_at = $3,
                updated_at = $3
        `;
        const values = [url, s3_key, new Date()];

        logger.debug(`Attempting to get db connection for URL: ${url}`);
        const dbClient = await getPgClient();
        logger.debug(`DB connection acquired. Executing mark as product page for URL: ${url}`);

        try {
            logger.debug(`Executing mark as product page query for URL: ${url}`);
            await dbClient.query(query, values);
            logger.info(`Marked as product page URL ${url} into database.`);
        } catch (err) {
            logger.error(err, `Error marking as product page URL ${url} into database.`);
            throw err;
        } finally {
            logger.debug(`Releasing db connection for URL: ${url}`);
            dbClient.release();
        }
    }

    public static async markAsNotProductPage(url: string, s3_key: string): Promise<void> {
        const query = `
            INSERT INTO page_classifications (url, s3_key, type, last_classified_at, updated_at)
            VALUES ($1, $2, 'not_product', $3, $3) ON CONFLICT (url) DO
            UPDATE SET type = 'not_product',
                s3_key = $2,
                last_classified_at = $3,
                updated_at = $3
        `;
        const values = [url, s3_key, new Date()];

        logger.debug(`Attempting to get db connection for URL: ${url}`);
        const dbClient = await getPgClient();
        logger.debug(`DB connection acquired. Executing mark as not product page for URL: ${url}`);

        try {
            logger.debug(`Executing mark as not product page query for URL: ${url}`);
            await dbClient.query(query, values);
            logger.info(`Marked as not product page URL ${url} into database.`);
        } catch (err) {
            logger.error(err, `Error marking as not product page URL ${url} into database.`);
            throw err;
        } finally {
            logger.debug(`Releasing db connection for URL: ${url}`);
            dbClient.release();
        }
    }

    public static async addProductTitleAndPrice(url: string, title: string, price: string): Promise<void> {
        const query = `
            INSERT INTO page_inferred_labels (url, product_title, product_price, last_inferred_at, updated_at)
            VALUES ($1, $2, $3, $4, $4) ON CONFLICT (url) DO
            UPDATE SET product_title = EXCLUDED.product_title,
                product_price = EXCLUDED.product_price,
                last_inferred_at = EXCLUDED.last_inferred_at,
                updated_at = EXCLUDED.updated_at
        `;
        const values = [url, title, price, new Date()];

        logger.debug(`Attempting to get db connection for URL: ${url}`);
        const dbClient = await getPgClient();
        logger.debug(`DB connection acquired. Executing add product title and price for URL: ${url}`);

        try {
            logger.debug(`Upserting product title and price for URL: ${url}`);
            await dbClient.query(query, values);
            logger.info(`Upserted product title and price for URL ${url} into database.`);
        } catch (err) {
            logger.error(err, `Error marking as not product page URL ${url} into database.`);
            throw err;
        } finally {
            logger.debug(`Releasing db connection for URL: ${url}`);
            dbClient.release();
        }
    }

    public static async addProductImage(url: string, image: string): Promise<void> {
        const query = `
            INSERT INTO page_inferred_labels (url, product_image_url, last_inferred_at, updated_at)
            VALUES ($1, $2, $3, $3) ON CONFLICT (url) DO
            UPDATE SET product_image_url = EXCLUDED.product_image_url,
                last_inferred_at = EXCLUDED.last_inferred_at,
                updated_at = EXCLUDED.updated_at
        `;
        const values = [url, image, new Date()];

        logger.debug(`Attempting to get db connection for URL: ${url}`);
        const dbClient = await getPgClient();
        logger.debug(`DB connection acquired. Executing add product image for URL: ${url}`);

        try {
            logger.debug(`Upserting product image for URL: ${url}`);
            await dbClient.query(query, values);
            logger.info(`Upserted product image for URL ${url} into database.`);
        } catch (err) {
            logger.error(err, `Error upserting product image for URL ${url} into database.`);
            throw err;
        } finally {
            logger.debug(`Releasing db connection for URL: ${url}`);
            dbClient.release();
        }
    }
}
