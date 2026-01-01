import { PageStorageConstructor } from './PageStorage/PageStorage';

const DEFAULT_MAX_REQUESTS_PER_CRAWL = 10;
const DEFAULT_MAX_CONCURRENCY = 5;
const DEFAULT_MAX_REQUESTS_PER_MINUTE = 50;
const DEFAULT_START_URL = 'https://buyabans.com';
const DEFAULT_PAGE_STORAGE = 'LocalStorage';

function getMaxRequestsPerCrawl(): number {
    const maxRequestsPerCrawl = process.env.MAX_REQUESTS_PER_CRAWL;
    if (maxRequestsPerCrawl) {
        const parsed = parseInt(maxRequestsPerCrawl, 10);
        if (!isNaN(parsed) && parsed > 0) {
            return parsed;
        }
    }
    return DEFAULT_MAX_REQUESTS_PER_CRAWL;
}

function getMaxConcurrency(): number {
    const maxConcurrency = process.env.MAX_CONCURRENCY;
    if (maxConcurrency) {
        const parsed = parseInt(maxConcurrency, 10);
        if (!isNaN(parsed) && parsed > 0) {
            return parsed;
        }
    }
    return DEFAULT_MAX_CONCURRENCY;
}

function getMaxRequestsPerMinute(): number {
    const maxRequestsPerMinute = process.env.MAX_REQUESTS_PER_MINUTE;
    if (maxRequestsPerMinute) {
        const parsed = parseInt(maxRequestsPerMinute, 10);
        if (!isNaN(parsed) && parsed > 0) {
            return parsed;
        }
    }
    return DEFAULT_MAX_REQUESTS_PER_MINUTE;
}

function getStartUrl(): string {
    const startUrl = process.env.START_URL;
    if (startUrl && startUrl.trim() !== '') {
        return startUrl;
    }
    return DEFAULT_START_URL;
}

function getPageStorageConstructor(): PageStorageConstructor {
    let pageStorage = process.env.PAGE_STORAGE;
    if (!pageStorage || pageStorage.trim() === '') {
        pageStorage = DEFAULT_PAGE_STORAGE;
    }

    switch (pageStorage) {
        case 'LocalStorage':
            return require('./storage/LocalStorage').default as PageStorageConstructor;
        default:
            throw new Error(`Unknown PAGE_STORAGE type: ${pageStorage}`);
    }
}

export { getMaxRequestsPerCrawl, getMaxConcurrency, getMaxRequestsPerMinute, getStartUrl, getPageStorageConstructor };
