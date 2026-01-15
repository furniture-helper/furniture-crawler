import { PlaywrightCrawler } from 'crawlee';
import {
    getMaxConcurrency,
    getMaxRequestsPerCrawl,
    getMaxRequestsPerMinute,
    getPageStorageConstructor,
} from './config';

import { getSpecialization } from './Specializations/Specialization';
import logger from './Logger';

export default class Crawler {
    private readonly crawler: PlaywrightCrawler;

    constructor() {
        const pageStorageConstructor = getPageStorageConstructor();

        this.crawler = new PlaywrightCrawler({
            maxRequestsPerCrawl: getMaxRequestsPerCrawl(),
            maxConcurrency: getMaxConcurrency(),
            maxRequestsPerMinute: getMaxRequestsPerMinute(),

            async requestHandler({ request, page, enqueueLinks }) {
                await page.waitForLoadState('load');

                // wait for network to be idle (or timeout after 10 seconds)
                await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {
                    logger.warn(`Network idle timeout for ${request.loadedUrl}`);
                });

                logger.info(`Parsing page: ${request.loadedUrl}`);

                // A specialization is a set of custom actions that will be applied to a page from a specific website.
                // For example, hiding pop-ups, closing modals, or any other action that improves data extraction.
                const specialization = await getSpecialization(request.loadedUrl, page);
                if (specialization) {
                    await specialization.apply();
                }

                // Store the page using the selected storage mechanism
                const storage = new pageStorageConstructor(request.loadedUrl, page);
                await storage.store();

                await enqueueLinks();
            },

            async errorHandler({ request, error: Error }) {
                const error = Error as Error;
                if (error.message.includes('Download is starting')) {
                    logger.info(`Skipping download: ${request.url}`);
                    request.noRetry = true;
                }
            },
        });
    }

    public async run(startUrl: string) {
        await this.crawler.run([startUrl]);
    }
}
