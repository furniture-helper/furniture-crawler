import Crawler from './Crawler';
import { getStartUrl } from './config';
import logger from './Logger';

async function main() {
    const crawler = new Crawler();
    const startUrl = getStartUrl();
    await crawler.run(startUrl);
}

main().catch((err) => {
    logger.error(`Unhandled error in main: ${err}`);
    process.exit(1);
});
