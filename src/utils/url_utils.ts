import logger from '../Logger';

export function getDomainFromUrl(url: string): string {
    try {
        const parsedUrl = new URL(url);
        const hostname = parsedUrl.hostname;
        return hostname.startsWith('www.') ? hostname.slice(4) : hostname;
    } catch (err) {
        logger.error(err, `Failed to parse domain from URL: ${url}`);
        return 'unknown';
    }
}
