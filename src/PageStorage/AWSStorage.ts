import { PageStorage } from './PageStorage';
import { S3Client } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import logger from '../Logger';
import { getPgClient } from '../db/pgClient';
import { getDomainFromUrl } from '../utils/url_utils';

const DEFAULT_S3_BUCKET = 'furniture-crawler-storage';
const DEFAULT_S3_REGION = 'eu-west-1';

export default class AWSStorage extends PageStorage {
    private static readonly region: string = process.env.AWS_REGION || DEFAULT_S3_REGION;
    private static readonly bucket: string = process.env.AWS_S3_BUCKET || DEFAULT_S3_BUCKET;
    private static readonly s3: S3Client = new S3Client({ region: AWSStorage.region });

    async store(): Promise<void> {
        const pageHtml = await this.page.content();
        const safeFileName = this.url.replace(/[^a-z0-9]/gi, '_').toLowerCase();
        const key = `${safeFileName}.html`;
        await this.uploadToS3(key, pageHtml);
        await this.upsertToDatabase(key);
    }

    private async uploadToS3(key: string, content: string): Promise<void> {
        const upload = new Upload({
            client: AWSStorage.s3,
            params: {
                Bucket: AWSStorage.bucket,
                Key: key,
                Body: Buffer.from(content, 'utf8'),
                ContentType: 'text/html; charset=utf-8',
            },
        });

        try {
            await upload.done();
            const fileUrl = `https://${AWSStorage.bucket}.s3.${AWSStorage.region}.amazonaws.com/${key}`;
            logger.info(`Stored page at URL: ${this.url} to S3: ${fileUrl}`);
        } catch (err) {
            logger.error(
                err,
                `Failed to store page at URL: ${this.url} to S3 bucket ${AWSStorage.bucket} with key ${key}`,
            );
            throw err;
        }
    }

    private async upsertToDatabase(s3Key: string): Promise<void> {
        const query = `
            INSERT INTO pages (url, domain, s3_key, is_active)
            VALUES ($1, $2, $3, true)
            ON CONFLICT (url) DO UPDATE
            SET s3_key = EXCLUDED.s3_key,
                is_active = true;
        `;
        const values = [this.url, getDomainFromUrl(this.url), s3Key];

        const dbClient = await getPgClient();
        try {
            await dbClient.query(query, values);
            logger.info(`Upserted page record for URL: ${this.url} into database.`);
        } catch (err) {
            logger.error(err, `Failed to upsert page record for URL: ${this.url} into database.`);
            throw err;
        } finally {
            dbClient.release();
        }
    }
}
