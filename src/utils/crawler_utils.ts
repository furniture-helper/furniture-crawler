import { Page } from 'playwright';
import logger from '../Logger';
import { getDomainFromUrl } from './url_utils';
import DatabaseUpsertQueue from '../db/DBUpsertQueue';
import { PlaywrightCrawlingContext, playwrightUtils } from 'crawlee';
import { Queue } from '../CrawlerQueue/Queue';
import { ALLOWED_DOMAINS } from '../allowed_domains';
import { transformUrl } from './url_transformers';
import { AbortedRequestError } from '../errors';

export async function checkForBlackListedUrl({ request }: PlaywrightCrawlingContext): Promise<void> {
    if (isBlacklistedUrl(request.url)) {
        logger.debug(`Blacklisted URL detected, skipping: ${request.url}`);
        request.noRetry = true;
        request.userData = { ...(request.userData || {}), isDownload: true };
        request.skipNavigation = true;

        // Remove from database
        await DatabaseUpsertQueue.deleteFromDatabase(request.url);
        await Queue.deleteMessage(request.url);

        throw new AbortedRequestError(request.url);
    }
}

function normalizeUrlForComparison(url: string): string {
    try {
        const parsed = new URL(url);
        // Remove trailing slashes from the pathname, but keep root as "/"
        let pathname = parsed.pathname.replace(/\/+$/, '');
        if (pathname === '') {
            pathname = '/';
        }
        parsed.pathname = pathname;
        // Build a canonical string without fragment (hash is ignored by Playwright page.url())
        return `${parsed.protocol}//${parsed.host}${parsed.pathname}${parsed.search}`;
    } catch {
        // Fallback: simple trailing slash normalization
        return url.replace(/\/+$/, '');
    }
}

export async function checkForRedirect({ page, request }: PlaywrightCrawlingContext): Promise<void> {
    if (request.skipNavigation) {
        return;
    }

    const originalUrl = request.url;
    const finalUrl = page.url();

    const normalizedOriginalUrl = normalizeUrlForComparison(originalUrl);
    const normalizedFinalUrl = normalizeUrlForComparison(finalUrl);

    if (normalizedFinalUrl !== normalizedOriginalUrl) {
        logger.debug(`Redirect detected from ${originalUrl} to ${finalUrl}`);
        await DatabaseUpsertQueue.markAsCrawled(originalUrl);
        await Queue.deleteMessage(originalUrl);
        request.url = finalUrl;
        // Ensure the rest of the crawler logic sees the correct redirected URL
        (request as any).loadedUrl = finalUrl;

        if (isBlacklistedUrl(finalUrl)) {
            logger.debug(`Blacklisted URL detected, skipping: ${finalUrl}`);
            await DatabaseUpsertQueue.deleteFromDatabase(finalUrl);
            await Queue.deleteMessage(finalUrl);
            request.noRetry = true;
            request.userData = { ...(request.userData || {}), isDownload: true };
        }
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

export async function waitForDomContentLoaded({ request, page }: PlaywrightCrawlingContext): Promise<void> {
    if (request.skipNavigation) return;
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
    const checkoutsPattern = /\/checkouts(?:\/|$)/i;
    const collectionsProductsPattern = /\/collections\/[^\/]+\/products\//i;
    const authPattern = /\/auth\/[^\/\?#]+\/?$/i;
    const fireworksUgandaPattern = /^https?:\/\/fireworks\.lk\/.*uganda/i;
    const assetsPattern = /\/assets\//i;

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
        checkoutsPattern,
        collectionsProductsPattern,
        authPattern,
        fireworksUgandaPattern,
        assetsPattern,
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
    const baseUrl = page.url(); // Use actual page URL as base for resolving relative URLs

    const sameDomainUrls = await page.$$eval(
        'a[href]',
        (anchors: HTMLAnchorElement[], { host, base }: { host: string; base: string }) => {
            const results: string[] = [];
            for (const a of anchors) {
                try {
                    // Get the href attribute value
                    const hrefAttr = a.getAttribute('href');
                    if (!hrefAttr) continue;

                    // Resolve relative URLs against the base URL
                    const absoluteUrl = new URL(hrefAttr, base).href.split('#')[0];
                    if (!absoluteUrl) continue;

                    const urlHostname = new URL(absoluteUrl).hostname;
                    if (urlHostname === host) {
                        results.push(absoluteUrl);
                    }
                } catch {
                    // Invalid URL, skip
                }
            }
            return [...new Set(results)];
        },
        { host: currentHost, base: baseUrl },
    );
    logger.debug(`Found ${sameDomainUrls.length} same-domain links on ${sourceUrl}`);

    for (let url of sameDomainUrls) {
        url = transformUrl(url);
        DatabaseUpsertQueue.checkAndInsertNewUrl(url).catch((err) => {
            logger.error(err, `Error checking/inserting URL: ${url}`);
        });
    }
}

export async function isUselessPage(url: string, page: Page): Promise<boolean> {
    const pageText = (await page.textContent('body')) || '';
    if (pageText.trim().length < 50) {
        logger.debug(`Page at ${url} deemed useless due to insufficient text content.`);
        return true;
    }
    return false;
}

export async function removeCommonElements(page: Page): Promise<void> {
    const knownClasses = ['.wd-products-nav'];
    await page.evaluate((classes) => {
        classes.forEach((cls) => {
            const elements = document.querySelectorAll(cls);
            elements.forEach((el) => el.remove());
        });
    }, knownClasses);
}

// export async function hideHiddenElements(page: Page): Promise<void> {
//     await page.evaluate(() => {
//         const hiddenElements = document.querySelectorAll('body *:not(script):not(noscript)');
//         hiddenElements.forEach((el) => {
//             const style = window.getComputedStyle(el);
//             if (style && (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0')) {
//                 el.remove(); // Actually removes from DOM
//             }
//         });
//     });
// }

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
