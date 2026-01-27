import { Configuration, PlaywrightCrawler, PlaywrightCrawlingContext, playwrightUtils } from 'crawlee';
import {
    getMaxConcurrency,
    getMaxRequestsPerCrawl,
    getMaxRequestsPerMinute,
    getPageStorageConstructor,
} from './config';

import { getSpecialization } from './Specializations/Specialization';
import logger from './Logger';
import DatabaseUpsertQueue from './db/DBUpsertQueue';
import { Page } from 'playwright';
import { addNewUrls, isBlacklistedUrl } from './utils/crawler_utils';

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

    private readonly completedCallbackFn: (url: string) => Promise<void>;
    private readonly pageStorageConstructor = getPageStorageConstructor();

    constructor(completedCallback: (url: string) => Promise<void>) {
        this.completedCallbackFn = completedCallback;

        this.crawler = new PlaywrightCrawler({
            ...this.settings,
            preNavigationHooks: [
                this.checkForBlackListedUrl.bind(this),
                this.waitForDomContentLoaded.bind(this),
                this.blockAds.bind(this),
                this.blockIframes.bind(this),
                this.blockUnncessaryResources.bind(this),
            ],

            requestHandler: this.requestHandler.bind(this),
            failedRequestHandler: this.failedRequestHandler.bind(this),
        });
    }

    private static async isUselessPage(url: string, page: Page): Promise<boolean> {
        const pageText = (await page.textContent('body')) || '';
        if (pageText.trim().length < 50) {
            logger.info(`Page at ${url} deemed useless due to insufficient text content.`);
            return true;
        }
        return false;
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

    private async checkForBlackListedUrl({ request }: PlaywrightCrawlingContext): Promise<void> {
        if (isBlacklistedUrl(request.url)) {
            logger.info(`Blacklisted URL detected, skipping: ${request.url}`);
            request.noRetry = true;
            request.userData = { ...(request.userData || {}), isDownload: true };
            request.skipNavigation = true;

            // Remove from database
            await DatabaseUpsertQueue.removeFromDatabase(request.url);
            await this.completedCallbackFn(request.url);
        }
    }

    private async blockAds({ blockRequests }: PlaywrightCrawlingContext): Promise<void> {
        await blockRequests({
            extraUrlPatterns: ['googletagservices.com', 'doubleclick.net', 'adsbygoogle.js', 'facebook.net'],
        });
    }

    private async blockIframes({ page }: PlaywrightCrawlingContext): Promise<void> {
        await page.route('**/*', (route) => {
            const type = route.request().resourceType();
            if (type === 'sub_frame') {
                return route.abort(); // Blocks all iframes
            }
            return route.continue();
        });
    }

    private async blockUnncessaryResources({ page }: PlaywrightCrawlingContext): Promise<void> {
        await playwrightUtils.blockRequests(page);
    }

    private async waitForDomContentLoaded({ page }: PlaywrightCrawlingContext): Promise<void> {
        await page.waitForLoadState('domcontentloaded');
    }

    private async requestHandler({ request, page }: PlaywrightCrawlingContext): Promise<void> {
        if (request.userData?.isDownload) {
            return;
        }

        if (!request.loadedUrl) {
            logger.error(`No loaded URL for request: ${request.url}`);
            await DatabaseUpsertQueue.removeFromDatabase(request.url);
            await this.completedCallbackFn(request.url);
            return;
        }

        const startTime = Date.now();

        logger.info(`Parsing page: ${request.loadedUrl}`);
        await page.route('**/*.{png,jpg,jpeg,gif,css,woff}', (route) => route.abort());
        await page.waitForLoadState('load');

        // wait for network to be idle (or timeout after 5 seconds)
        await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {
            logger.warn(`Network idle timeout for ${request.loadedUrl}`);
        });

        logger.info(`Page loaded: ${request.loadedUrl} in ${Date.now() - startTime} ms`);

        // Check if the page is considered "useless" and should not be crawled
        if (await Crawler.isUselessPage(request.loadedUrl, page)) {
            logger.info(`Skipping useless page: ${request.loadedUrl}`);
            await DatabaseUpsertQueue.removeFromDatabase(request.loadedUrl);
            await this.completedCallbackFn(request.url);
            return;
        }

        // Resolve all relative URLs to absolute URLs
        await page.evaluate(() => {
            const resolveToAbsolute = (attrName: string, propName: string) => {
                const selector = attrName === 'src' ? `[${attrName}]:not(script)` : `[${attrName}]`;

                const elements = document.querySelectorAll(selector);
                elements.forEach((el) => {
                    const element = el as any;

                    const absoluteUrl = element[propName];

                    if (typeof absoluteUrl === 'string' && absoluteUrl.trim() !== '') {
                        element.setAttribute(attrName, absoluteUrl);
                    }
                });
            };

            resolveToAbsolute('href', 'href');
            resolveToAbsolute('src', 'src');
            resolveToAbsolute('action', 'action');
            resolveToAbsolute('data', 'data');
        });
        logger.debug(`Resolved relative URLs to absolute for page: ${request.loadedUrl}`);

        // A specialization is a set of custom actions that will be applied to a page from a specific website.
        // For example, hiding pop-ups, closing modals, or any other action that improves data extraction.
        const specialization = await getSpecialization(request.loadedUrl, page);
        if (specialization) {
            logger.debug(`Resolving specialization for ${request.loadedUrl}`);
            await specialization.apply();
        }

        // Store the page using the selected storage mechanism
        logger.debug(`Working on storing page: ${request.loadedUrl}`);
        const storage = new this.pageStorageConstructor(request.loadedUrl, page);
        await storage.store();

        await this.completedCallbackFn(request.url);
        logger.info(`Completed processing for page: ${request.loadedUrl}`);

        await addNewUrls(request.loadedUrl, page).catch((err) => {
            logger.error(err, `Error adding new URLs from page: ${request.loadedUrl}`);
        });
    }

    private async failedRequestHandler({ request, error }: PlaywrightCrawlingContext): Promise<void> {
        logger.error(error, `Request failed for ${request.url}`);
        await DatabaseUpsertQueue.removeFromDatabase(request.url);
        await this.completedCallbackFn(request.url);
    }
}
