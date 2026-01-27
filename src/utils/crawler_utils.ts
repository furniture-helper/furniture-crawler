import { Page } from 'playwright';
import logger from '../Logger';
import { getDomainFromUrl } from './url_utils';
import DatabaseUpsertQueue from '../db/DBUpsertQueue';

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
        if (isBlacklistedUrl(url)) continue;
        DatabaseUpsertQueue.checkAndInsertNewUrl(url).catch((err) => {
            logger.error(err, `Error checking/inserting URL: ${url}`);
        });
    }
}
