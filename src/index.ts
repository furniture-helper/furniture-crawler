import Crawler from './Crawler';
import logger from './Logger';
import { Queue } from './CrawlerQueue/Queue';
import { getMaxRequestsPerCrawl } from './config';

const timeOutDuration = parseInt(process.env.TIMEOUT_MINS || '60', 10) * 1000 * 60;
const TIMEOUT_MESSAGE = `Timeout after ${timeOutDuration} ms`;

const receiptHandles: Map<string, string> = new Map();
const queue = new Queue();

const timeout = async (promise: Promise<void>, time: number) => {
    let timer: NodeJS.Timeout;
    try {
        return await Promise.race([
            promise,
            new Promise((resolve, reject) => (timer = setTimeout(reject, time, TIMEOUT_MESSAGE))),
        ]);
    } finally {
        clearTimeout(timer!);
    }
};

export async function deleteFromQueue(url: string) {
    const receiptHandle = receiptHandles.get(url);
    if (!receiptHandle) {
        logger.warn(`No receipt handle found for URL: ${url}. Cannot delete from queue.`);
        return;
    }

    try {
        await queue.deleteMessage(receiptHandle);
        logger.info(`Deleted URL from queue: ${url}`);
    } catch (err) {
        logger.error(
            `Error deleting URL ${url} from queue: ${err instanceof Error ? err.stack || err.message : String(err)}`,
        );
    }
}

async function main() {
    logger.debug(`Initializing crawler...`);
    const crawler = new Crawler();

    let totalRequestsQueued = 0;
    while (true) {
        const messages = await queue.getMessages();
        if (messages.length === 0) {
            logger.info('No messages left in queue... Exiting.');
            break;
        }

        for (const message of messages) {
            logger.debug(`Adding URL from queue: ${message.url}`);
            try {
                await crawler.add(message.url);
                receiptHandles.set(message.url, message.receiptHandle);
                totalRequestsQueued += 1;
            } catch (err) {
                logger.error(
                    `Error Adding URL ${message.url}: ${err instanceof Error ? err.stack || err.message : String(err)}`,
                );
            }
        }

        if (totalRequestsQueued >= getMaxRequestsPerCrawl()) {
            logger.info(`Reached max requests per crawl: ${getMaxRequestsPerCrawl()}`);
            break;
        }

        // Wait 5s
        await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    try {
        logger.info(`Starting crawler with ${totalRequestsQueued} URLs in queue`);
        await timeout(crawler.run(), timeOutDuration);

        logger.info(`Crawl completed successfully, exiting...`);

        // wait 10s
        logger.info(`Waiting 10s before exiting to allow for graceful shutdown...`);
        await new Promise((resolve) => setTimeout(resolve, 10000));

        process.exit(0);
    } catch (error) {
        if (error === TIMEOUT_MESSAGE) {
            logger.info(`Crawl timed out after ${timeOutDuration} ms, stopping crawler...`);
            crawler.stop('TIMEOUT');

            //wait 15s
            logger.info(`Waiting 15s before exiting to allow for graceful shutdown...`);
            await new Promise((resolve) => setTimeout(resolve, 15000));

            process.exit(0);
        }
        throw error;
    }
}

main().catch(async (err) => {
    const errorMessage = err instanceof Error ? err.stack || err.message : String(err);
    logger.error(`Unhandled error in main: ${errorMessage}`);
    process.exit(1);
});
