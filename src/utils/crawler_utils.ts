import { Page } from 'playwright';
import logger from '../Logger';
import { getDomainFromUrl } from './url_utils';
import DatabaseUpsertQueue from '../db/DBUpsertQueue';
import { PlaywrightCrawlingContext, playwrightUtils } from 'crawlee';
import { Queue } from '../CrawlerQueue/Queue';
import { ALLOWED_DOMAINS } from '../allowed_domains';

export async function checkForBlackListedUrl({ request }: PlaywrightCrawlingContext): Promise<void> {
    if (isBlacklistedUrl(request.url)) {
        logger.info(`Blacklisted URL detected, skipping: ${request.url}`);
        request.noRetry = true;
        request.userData = { ...(request.userData || {}), isDownload: true };
        request.skipNavigation = true;

        // Remove from database
        await DatabaseUpsertQueue.removeFromDatabase(request.url);
        await Queue.deleteMessage(request.url);
    }
}

export async function blockAds({ blockRequests }: PlaywrightCrawlingContext): Promise<void> {
    await blockRequests({
        extraUrlPatterns: ['googletagservices.com', 'doubleclick.net', 'adsbygoogle.js', 'facebook.net'],
    });
}

export async function blockIframes({ page }: PlaywrightCrawlingContext): Promise<void> {
    await page.route('**/*', (route) => {
        const type = route.request().resourceType();
        if (type === 'sub_frame') {
            return route.abort(); // Blocks all iframes
        }
        return route.continue();
    });
}

export async function blockUnnecessaryResources({ page }: PlaywrightCrawlingContext): Promise<void> {
    await playwrightUtils.blockRequests(page);
}

export async function waitForDomContentLoaded({ page }: PlaywrightCrawlingContext): Promise<void> {
    await page.waitForLoadState('domcontentloaded');
}

export function isBlacklistedUrl(url: string): boolean {
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

    if (!ALLOWED_DOMAINS.includes(getDomainFromUrl(url))) {
        logger.debug(`URL ${url} is blacklisted due to not being in allowed domains.`);
        return true;
    }

    const wishListPattern = /\/wishlist\/\d+\/addAj(?:\/|$)/;
    const addToCartPattern = /(?:[?&]|^)add-to-cart=(\d+)(?:&|$)/;
    const brochureDownloadPattern = /\/brochure\/download\/(?:[^?#\s]*)/;
    const sharePattern = /(?:[?&]|^)share=([^&]+)(?:&|$)/i;
    const wooComparePattern = /(?=.*[?&]action=yith-woocompare-add-product(?:&|$))(?=.*[?&]id=(?<id>\d+)(?:&|$)).*/i;
    const addToWishlistQueryPattern = /(?:[?&]|^)add_to_wishlist=(\d+)(?:&|$)/i;
    const productTagPattern = /\/product-tag\/[^\/?#]+\/?/i;

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
        productTagPattern,
    ];
    const matchesPattern = blacklistedPatterns.some((pattern) => pattern.test(url));
    if (matchesPattern) {
        logger.debug(`URL ${url} is blacklisted based on predefined patterns.`);
        return true;
    }

    return false;
}

export async function addNewUrls(sourceUrl: string, page: Page) {
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
        DatabaseUpsertQueue.checkAndInsertNewUrl(url).catch((err) => {
            logger.error(err, `Error checking/inserting URL: ${url}`);
        });
    }
}

export async function isUselessPage(url: string, page: Page): Promise<boolean> {
    const pageText = (await page.textContent('body')) || '';
    if (pageText.trim().length < 50) {
        logger.info(`Page at ${url} deemed useless due to insufficient text content.`);
        return true;
    }
    return false;
}

export async function resolveToAbsoluteUrls(page: Page): Promise<void> {
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
}
