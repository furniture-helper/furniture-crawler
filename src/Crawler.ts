import { PlaywrightCrawler } from 'crawlee';
import {
    getMaxConcurrency,
    getMaxRequestsPerCrawl,
    getMaxRequestsPerMinute,
    getPageStorageConstructor,
} from './config';

import { getSpecialization } from './Specializations/Specialization';

export default class Crawler {
    private readonly crawler: PlaywrightCrawler;

    constructor() {
        const pageStorageConstructor = getPageStorageConstructor();

        this.crawler = new PlaywrightCrawler({
            maxRequestsPerCrawl: getMaxRequestsPerCrawl(),
            maxConcurrency: getMaxConcurrency(),
            maxRequestsPerMinute: getMaxRequestsPerMinute(),

            async requestHandler({ request, page, enqueueLinks, log }) {
                await page.waitForLoadState('load');

                // wait for network to be idle (or timeout after 10 seconds)
                await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {
                    log.warning(`Network idle timeout for ${request.loadedUrl}`);
                });

                log.info(`Parsing page: ${request.loadedUrl}`);

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
        });
    }

    public async run(startUrl: string) {
        await this.crawler.run([startUrl]);
    }
}
