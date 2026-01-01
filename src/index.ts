import Crawler from './Crawler';
import { getStartUrl } from './config';
import logger from './Logger';

async function main() {
    const crawler = new Crawler();
    const startUrl = getStartUrl();
    await crawler.run(startUrl);
}

main().catch((err) => {
    const errorMessage =
        err instanceof Error
            ? err.stack || err.message
            : String(err);
    logger.error(`Unhandled error in main: ${errorMessage}`);
    process.exit(1);
});
