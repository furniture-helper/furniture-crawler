import Crawler from './Crawler';
import { getStartUrl } from './config';
import logger from './Logger';
import { gracefulShutdown } from './db/pgClient';

const timeOutDuration = parseInt(process.env.TIMEOUT_MINS || '30', 10) * 1000 * 60;
const TIMEOUT_MESSAGE = `Timeout after ${timeOutDuration} ms`;

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

async function main() {
    const crawler = new Crawler();
    const startUrl = getStartUrl();

    try {
        await timeout(crawler.run(startUrl), timeOutDuration);
    } catch (error) {
        if (error === TIMEOUT_MESSAGE) {
            crawler.stop('TIMEOUT');
            await gracefulShutdown('TIMEOUT');
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
