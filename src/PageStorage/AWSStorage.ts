import { PageStorage } from './PageStorage';
import { S3Client } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import logger from '../Logger';
import { createHash } from 'crypto';

const DEFAULT_S3_BUCKET = 'furniture-crawler-storage';
const DEFAULT_S3_REGION = 'eu-west-1';

export default class AWSStorage extends PageStorage {
    private static readonly region: string = process.env.AWS_REGION || DEFAULT_S3_REGION;
    private static readonly bucket: string = process.env.AWS_S3_BUCKET || DEFAULT_S3_BUCKET;
    private static readonly s3: S3Client = new S3Client({ region: AWSStorage.region });

    async store(): Promise<void> {
        const pageHtml = await this.page.content();

        // Create a hash of the URL to ensure uniqueness
        const urlHash = createHash('md5').update(this.url).digest('hex').substring(0, 8);
        const safeFileName = this.url.replace(/[^a-z0-9]/gi, '_').toLowerCase();
        const key = `${safeFileName}_${urlHash}.html`;

        const upload = new Upload({
            client: AWSStorage.s3,
            params: {
                Bucket: AWSStorage.bucket,
                Key: key,
                Body: Buffer.from(pageHtml, 'utf8'),
                ContentType: 'text/html; charset=utf-8',
            },
        });

        try {
            await upload.done();
            const fileUrl = `https://${AWSStorage.bucket}.s3.${AWSStorage.region}.amazonaws.com/${key}`;
            logger.info(`Stored page at URL: ${this.url} to S3: ${fileUrl}`);
        } catch (err) {
            logger.error(
                {
                    err,
                    bucket: AWSStorage.bucket,
                    key: key,
                },
                `Failed to store page at URL: ${this.url} to S3`,
            );
            throw err;
        }
    }
}
