import { Configuration, PlaywrightCrawler, PlaywrightCrawlingContext } from 'crawlee';
import {
    getMaxConcurrency,
    getMaxRequestsPerCrawl,
    getMaxRequestsPerMinute,
    getPageStorageConstructor,
} from './config';

import { getSpecialization } from './Specializations/Specialization';
import logger from './Logger';
import DatabaseUpsertQueue from './db/DBUpsertQueue';
import {
    addNewUrls,
    blockAds,
    blockIframes,
    blockUnnecessaryResources,
    checkForBlackListedUrl,
    isUselessPage,
    resolveToAbsoluteUrls,
    waitForDomContentLoaded,
} from './utils/crawler_utils';
import { Queue } from './CrawlerQueue/Queue';

Configuration.set('systemInfoV2', true);
Configuration.set('availableMemoryRatio', 0.8);
Configuration.set('maxUsedCpuRatio', 0.8);
Configuration.set('containerized', true);

export default class Crawler {
    private readonly crawler: PlaywrightCrawler;
    private readonly settings = {
        headless: true,
        maxRequestsPerCrawl: getMaxRequestsPerCrawl(),
        maxConcurrency: getMaxConcurrency(),
        maxRequestsPerMinute: getMaxRequestsPerMinute(),
        autoscaledPoolOptions: {
            desiredConcurrencyRatio: 0.8,
            maxConcurrency: getMaxConcurrency(),
        },
        requestHandlerTimeoutSecs: 30,
        persistCookiesPerSession: true,
        navigationTimeoutSecs: 30,
    };

    private readonly pageStorageConstructor = getPageStorageConstructor();

    constructor() {
        this.crawler = new PlaywrightCrawler({
            ...this.settings,
            preNavigationHooks: [
                checkForBlackListedUrl.bind(this),
                waitForDomContentLoaded.bind(this),
                blockAds.bind(this),
                blockIframes.bind(this),
                blockUnnecessaryResources.bind(this),
            ],

            requestHandler: this.requestHandler.bind(this),
            failedRequestHandler: this.failedRequestHandler.bind(this),
        });
    }

    public async run() {
        await this.crawler.run();
    }

    public async add(url: string) {
        await this.crawler.addRequests([url]);
    }

    public stop(reason: string) {
        this.crawler.stop(reason);
    }

    private async requestHandler({ request, page }: PlaywrightCrawlingContext): Promise<void> {
        if (request.userData?.isDownload) {
            await this.removeFromDatabaseAndQueue(request.url);
            return;
        }

        if (!request.loadedUrl) {
            logger.error(`No loaded URL for request: ${request.url}`);
            await this.removeFromDatabaseAndQueue(request.url);
            return;
        }

        const startTime = Date.now();

        logger.info(`Parsing page: ${request.loadedUrl}`);

        // Abort loading of unnecessary resources to speed up page load
        await page.route('**/*.{png,jpg,jpeg,gif,css,woff}', (route) => route.abort());

        await page.waitForLoadState('load');

        // wait for network to be idle (or timeout after 5 seconds)
        await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {
            logger.warn(`Network idle timeout for ${request.loadedUrl}`);
        });

        logger.info(`Page loaded: ${request.loadedUrl} in ${Date.now() - startTime} ms`);

        // Check if the page is considered "useless" and should not be crawled
        if (await isUselessPage(request.loadedUrl, page)) {
            logger.info(`Skipping useless page: ${request.loadedUrl}`);
            await this.removeFromDatabaseAndQueue(request.url);
            return;
        }

        // A specialization is a set of custom actions that will be applied to a page from a specific website.
        // For example, hiding pop-ups, closing modals, or any other action that improves data extraction.
        const specialization = await getSpecialization(request.loadedUrl, page);
        if (specialization) {
            logger.debug(`Resolving specialization for ${request.loadedUrl}`);
            await specialization.apply();
        }

        await resolveToAbsoluteUrls(page);
        logger.debug(`Resolved relative URLs to absolute for page: ${request.loadedUrl}`);

        // Store the page using the selected storage mechanism
        logger.debug(`Working on storing page: ${request.loadedUrl}`);
        const storage = new this.pageStorageConstructor(request.loadedUrl, page);
        await storage.store();

        await Queue.deleteMessage(request.url);
        logger.info(`Completed processing for page: ${request.loadedUrl}`);

        await addNewUrls(request.loadedUrl, page).catch((err) => {
            logger.error(err, `Error adding new URLs from page: ${request.loadedUrl}`);
        });
    }

    private async failedRequestHandler({ request, error }: PlaywrightCrawlingContext): Promise<void> {
        logger.error(error, `Request failed for ${request.url}`);
        await this.removeFromDatabaseAndQueue(request.url);
    }

    private async removeFromDatabaseAndQueue(url: string): Promise<void> {
        await DatabaseUpsertQueue.removeFromDatabase(url);
        await Queue.deleteMessage(url);
    }
}
