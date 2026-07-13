import logger from '../Logger';

const COLLECTIONS_PRODUCTS_REGEX = /\/collections\/[^\/]+\/products\//;

export function transformUrl(url: string): string {
    if (COLLECTIONS_PRODUCTS_REGEX.test(url)) {
        const transformedUrl = replaceCollectionsProducts(url);
        logger.debug(`Transformed URL from ${url} to ${transformedUrl}`);
        return transformedUrl;
    }
    url = keepOnlyFirstQueryParam(url);
    return url;
}

// If a URL contains more than one query parameter, strip all but the first.
// e.g. https://example.com/page?foo=1&bar=2&baz=3 → https://example.com/page?foo=1
function keepOnlyFirstQueryParam(url: string): string {
    const qIndex = url.indexOf('?');
    if (qIndex === -1) return url;
    const base = url.substring(0, qIndex);
    const queryString = url.substring(qIndex + 1);
    const params = queryString.split('&');
    if (params.length <= 1) return url;
    const transformed = `${base}?${params[0]}`;
    logger.debug(`Trimmed query params of URL from ${url} to ${transformed}`);
    return transformed;
}

// ...existing code...
// Converts a collections products URL to a products URL.
// For example:
// https://www.simplytek.lk/collections/huawei-smart-wearables/products/huawei-band-10-smartwatch
// becomes
// https://www.simplytek.lk/products/huawei-band-10-smartwatch
function replaceCollectionsProducts(url: string): string {
    return url.replace(COLLECTIONS_PRODUCTS_REGEX, '/products/');
}
