import { PageStorageConstructor } from './PageStorage/PageStorage';

const DEFAULT_MAX_REQUESTS_PER_CRAWL = 10;
const DEFAULT_MAX_CONCURRENCY = 3;
const DEFAULT_MAX_REQUESTS_PER_MINUTE = 50;
const DEFAULT_PAGE_STORAGE = 'LocalStorage';
const DEFAULT_REQUEST_HANDLER_TIMEOUT_S = 30;
const DEFAULT_NAVIGATION_TIMEOUT_S = 30;

export function getMaxRequestsPerCrawl(): number {
    const maxRequestsPerCrawl = process.env.MAX_REQUESTS_PER_CRAWL;
    if (maxRequestsPerCrawl) {
        const parsed = parseInt(maxRequestsPerCrawl, 10);
        if (!isNaN(parsed) && parsed > 0) {
            return parsed;
        }
    }
    return DEFAULT_MAX_REQUESTS_PER_CRAWL;
}

export function getMaxConcurrency(): number {
    const maxConcurrency = process.env.MAX_CONCURRENCY;
    if (maxConcurrency) {
        const parsed = parseInt(maxConcurrency, 10);
        if (!isNaN(parsed) && parsed > 0) {
            return parsed;
        }
    }
    return DEFAULT_MAX_CONCURRENCY;
}

export function getMaxRequestsPerMinute(): number {
    const maxRequestsPerMinute = process.env.MAX_REQUESTS_PER_MINUTE;
    if (maxRequestsPerMinute) {
        const parsed = parseInt(maxRequestsPerMinute, 10);
        if (!isNaN(parsed) && parsed > 0) {
            return parsed;
        }
    }
    return DEFAULT_MAX_REQUESTS_PER_MINUTE;
}

export function getPageStorageConstructor(): PageStorageConstructor {
    let pageStorage = process.env.PAGE_STORAGE;
    if (!pageStorage || pageStorage.trim() === '') {
        pageStorage = DEFAULT_PAGE_STORAGE;
    }

    switch (pageStorage) {
        case 'LocalStorage':
            return require('./PageStorage/LocalStorage').default as PageStorageConstructor;
        case 'AWSStorage':
            return require('./PageStorage/AWSStorage').default as PageStorageConstructor;
        default:
            throw new Error(`Unknown PAGE_STORAGE type: ${pageStorage}`);
    }
}

export function getRequestHandlerTimeoutSecs(): number {
    const timeout = process.env.REQUEST_HANDLER_TIMEOUT_S;
    if (timeout) {
        const parsed = parseInt(timeout, 10);
        if (!isNaN(parsed) && parsed > 0) {
            return parsed;
        }
    }
    return DEFAULT_REQUEST_HANDLER_TIMEOUT_S;
}

export function getNavigationTimeoutSecs(): number {
    const timeout = process.env.NAVIGATION_TIMEOUT_S;
    if (timeout) {
        const parsed = parseInt(timeout, 10);
        if (!isNaN(parsed) && parsed > 0) {
            return parsed;
        }
    }
    return DEFAULT_NAVIGATION_TIMEOUT_S;
}
