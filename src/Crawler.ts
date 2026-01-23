import { Configuration, PlaywrightCrawler, playwrightUtils } from 'crawlee';
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
import { getDomainFromUrl } from './utils/url_utils';

Configuration.set('systemInfoV2', true);
Configuration.set('memoryMbytes', 8192);
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
                requestHandlerTimeoutSecs: 30,
                persistCookiesPerSession: true,
                navigationTimeoutSecs: 30,

                preNavigationHooks: [
                    // Handle blacklisted URLs
                    async ({ request }) => {
                        if (Crawler.isBlacklistedUrl(request.url)) {
                            logger.info(`Blacklisted URL detected, skipping: ${request.url}`);
                            request.noRetry = true;
                            request.userData = { ...(request.userData || {}), isDownload: true };
                            request.skipNavigation = true;

                            // Remove from database
                            await DatabaseUpsertQueue.removeFromDatabase(request.url);
                            await completedCallback(request.url);
                        }
                    },

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
                ],

                async requestHandler({ request, page }) {
                    if (request.userData?.isDownload) {
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
                        await completedCallback(request.url);
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
                    const storage = new pageStorageConstructor(request.loadedUrl, page);
                    await storage.store();

                    await completedCallback(request.url);
                    logger.info(`Completed processing for page: ${request.loadedUrl}`);

                    await Crawler.addNewUrls(request.loadedUrl, page).catch((err) => {
                        logger.error(err, `Error adding new URLs from page: ${request.loadedUrl}`);
                    });
                },

                failedRequestHandler: async ({ request, error }) => {
                    logger.error(error, `Request failed for ${request.url}`);
                    await DatabaseUpsertQueue.removeFromDatabase(request.url);
                    await completedCallback(request.url);
                },
            },
            new Configuration({
                availableMemoryRatio: 0.8,
                maxUsedCpuRatio: 0.8,
                disableBrowserSandbox: true,
                memoryMbytes: 8192,
                systemInfoV2: true,
                containerized: true,
            }),
        );
    }

    private static async addNewUrls(sourceUrl: string, page: Page) {
        const currentHost = new URL(sourceUrl).hostname;

        const sameDomainUrls = await page.$$eval(
            'a[href]',
            (anchors: HTMLAnchorElement[], host: string) =>
                Array.from(
                    new Set(
                        anchors
                            .map((a) => a.href.split('#')[0]) // remove fragments
                            .filter(Boolean)
                            .filter((h) => {
                                try {
                                    return new URL(h).hostname === host;
                                } catch {
                                    return false;
                                }
                            }),
                    ),
                ),
            currentHost,
        );
        logger.info(`Found ${sameDomainUrls.length} same-domain links on ${sourceUrl}`);

        for (let url of sameDomainUrls) {
            if (Crawler.isBlacklistedUrl(url)) continue;
            DatabaseUpsertQueue.checkAndInsertNewUrl(url).catch((err) => {
                logger.error(err, `Error checking/inserting URL: ${url}`);
            });
        }
    }

    private static async isUselessPage(url: string, page: Page): Promise<boolean> {
        const pageText = (await page.textContent('body')) || '';
        if (pageText.trim().length < 50) {
            logger.info(`Page at ${url} deemed useless due to insufficient text content.`);
            return true;
        }
        return false;
    }

    private static isBlacklistedUrl(url: string): boolean {
        const doesUrlContainQueryParam = url.includes('?') || url.includes('&');
        if (doesUrlContainQueryParam) {
            logger.debug(`URL ${url} is blacklisted due to containing query parameters.`);
            return true;
        }

        const doesUrlContainExtension =
            /\.(jpg|jpeg|png|gif|bmp|svg|webp|mp4|mp3|avi|mov|wmv|flv|mkv|pdf|docx?|xlsx?|pptx?|zip|rar|7z|avif)(?:[?#]|$)/i.test(
                url,
            );
        if (doesUrlContainExtension) {
            logger.debug(`URL ${url} is blacklisted due to containing a file extension.`);
            return true;
        }

        const allowed_domains = [
            'www.damro.lk',
            'www.singersl.com',
            'strong.lk',
            'singhagiri.lk',
            'ugreen.lk',
            'mysoftlogic.lk',
            'raesl.lk',
            'www.nanotek.lk',
            'lifemobile.lk',
            'fireworks.lk',
            'www.simplytek.lk',
        ];
        if (!allowed_domains.includes(getDomainFromUrl(url))) {
            logger.debug(`URL ${url} is blacklisted due to not being in allowed domains.`);
            return true;
        }

        const wishListPattern = /\/wishlist\/\d+\/addAj(?:\/|$)/;
        const addToCartPattern = /(?:[?&]|^)add-to-cart=(\d+)(?:&|$)/;
        const brochureDownloadPattern = /\/brochure\/download\/(?:[^?#\s]*)/;
        const sharePattern = /(?:[?&]|^)share=([^&]+)(?:&|$)/i;
        const wooComparePattern =
            /(?=.*[?&]action=yith-woocompare-add-product(?:&|$))(?=.*[?&]id=(?<id>\d+)(?:&|$)).*/i;
        const addToWishlistQueryPattern = /(?:[?&]|^)add_to_wishlist=(\d+)(?:&|$)/i;

        const blacklistedPatterns = [
            /\/auth\/?$/i,
            /\/login\/?$/i,
            /\/signup\/?$/i,
            /\/register\/?$/i,
            /\/cart\/?$/i,
            /\/checkout\/?$/i,
            /\/user\/profile\/?$/i,
            wishListPattern,
            addToCartPattern,
            brochureDownloadPattern,
            sharePattern,
            wooComparePattern,
            addToWishlistQueryPattern,
        ];
        const matchesPattern = blacklistedPatterns.some((pattern) => pattern.test(url));
        if (matchesPattern) {
            logger.debug(`URL ${url} is blacklisted based on predefined patterns.`);
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
}
