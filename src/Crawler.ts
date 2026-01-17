import { Configuration, PlaywrightCrawler, playwrightUtils } from 'crawlee';
import {
    getMaxConcurrency,
    getMaxRequestsPerCrawl,
    getMaxRequestsPerMinute,
    getPageStorageConstructor,
} from './config';

import { getSpecialization } from './Specializations/Specialization';
import logger from './Logger';

Configuration.set('systemInfoV2', true);
Configuration.set('memoryMbytes', 16384);
Configuration.set('containerized', true);
Configuration.set('availableMemoryRatio', 0.8);
Configuration.set('maxUsedCpuRatio', 0.8);
Configuration.set('disableBrowserSandbox', true);
Configuration.set('containerized', true);

export default class Crawler {
    private readonly crawler: PlaywrightCrawler;

    constructor(completedCallback: (url: string) => Promise<void>) {
        const pageStorageConstructor = getPageStorageConstructor();

        this.crawler = new PlaywrightCrawler(
            {
                headless: true,
                maxRequestsPerCrawl: getMaxRequestsPerCrawl(),
                maxConcurrency: getMaxConcurrency(),
                maxRequestsPerMinute: getMaxRequestsPerMinute(),
                autoscaledPoolOptions: {
                    desiredConcurrencyRatio: 0.8,
                    maxConcurrency: getMaxConcurrency(),
                },
                requestHandlerTimeoutSecs: 20,
                persistCookiesPerSession: false,
                navigationTimeoutSecs: 10,

                preNavigationHooks: [
                    // Wait for DOM content to be loaded before proceeding
                    (context, gotoOptions) => {
                        gotoOptions.waitUntil = 'domcontentloaded';
                    },

                    // Block unnecessary resources to speed up crawling
                    async ({ page }) => {
                        // This single line blocks images, fonts, css, and media
                        await playwrightUtils.blockRequests(page);
                    },

                    async ({ blockRequests }) => {
                        await blockRequests({
                            // Blocks images, css, and fonts by default.
                            // You can add custom ad-server patterns here:
                            extraUrlPatterns: [
                                'googletagservices.com',
                                'doubleclick.net',
                                'adsbygoogle.js',
                                'facebook.net',
                            ],
                        });
                    },

                    async ({ page }) => {
                        await page.route('**/*', (route) => {
                            const type = route.request().resourceType();
                            if (type === 'sub_frame') {
                                return route.abort(); // Blocks all iframes
                            }
                            return route.continue();
                        });
                    },

                    async ({ page }) => {
                        await page.route('**/*', async (route) => {
                            const headers = route.request().headers();
                            delete headers['cookie']; // Prevent sending cookies
                            await route.continue({ headers });
                        });
                    },

                    // Intercept responses to detect downloads via headers
                    async ({ page, request }) => {
                        await page.route(request.url, async (route) => {
                            try {
                                // Fetch the response manually to check headers
                                const response = await route.fetch();
                                const headers = response.headers();

                                // Check triggers: "attachment" in disposition OR common file types
                                const contentDisposition = headers['content-disposition'] || '';
                                const contentType = headers['content-type'] || '';

                                const isDownload =
                                    contentDisposition.includes('attachment') ||
                                    /application\/pdf|application\/zip|application\/octet-stream/.test(contentType);

                                if (isDownload) {
                                    logger.info(`Download detected (${contentType}): ${request.url}`);
                                    request.noRetry = true;
                                    request.userData = { ...(request.userData || {}), isDownload: true };
                                    request.skipNavigation = true;

                                    // Fulfill with a simple response to skip actual download
                                    await route.fulfill({
                                        status: 200,
                                        contentType: 'text/html; charset=utf-8',
                                        body: '<html><body>Download skipped</body></html>',
                                    });
                                } else {
                                    await route.fulfill({ response });
                                }
                            } catch {
                                // If the manual fetch fails (e.g. network error), let default behavior take over
                                route.continue().catch(() => {});
                            }
                        });
                    },
                ],

                async requestHandler({ request, page }) {
                    if (request.userData?.isDownload) {
                        logger.info(`Skipping processing for download URL: ${request.url}`);
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
                    const storage = new pageStorageConstructor(request.loadedUrl, page);
                    await storage.store();

                    await completedCallback(request.url);
                    logger.info(`Completed processing for page: ${request.loadedUrl}`);
                    await page.close();
                    logger.debug(`Closed page: ${request.loadedUrl}`);
                },
            },
            new Configuration({
                availableMemoryRatio: 0.8,
                maxUsedCpuRatio: 0.8,
                disableBrowserSandbox: true,
                memoryMbytes: 16384,
                systemInfoV2: true,
                containerized: true,
            }),
        );
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
}
