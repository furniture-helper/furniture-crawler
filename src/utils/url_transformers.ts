import logger from '../Logger';

const COLLECTIONS_PRODUCTS_REGEX = /\/collections\/[^\/]+\/products\//;

export function transformUrl(url: string): string {
    if (COLLECTIONS_PRODUCTS_REGEX.test(url)) {
        const transformedUrl = replaceCollectionsProducts(url);
        logger.debug(`Transformed URL from ${url} to ${transformedUrl}`);
        return transformedUrl;
    }
    return url;
}

// Converts a collections products URL to a products URL.
// For example:
// https://www.simplytek.lk/collections/huawei-smart-wearables/products/huawei-band-10-smartwatch
// becomes
// https://www.simplytek.lk/products/huawei-band-10-smartwatch
function replaceCollectionsProducts(url: string): string {
    return url.replace(COLLECTIONS_PRODUCTS_REGEX, '/products/');
}
