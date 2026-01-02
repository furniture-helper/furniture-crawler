import logger from '../Logger';

export function getDomainFromUrl(url: string): string {
    try {
        const parsedUrl = new URL(url);
        return parsedUrl.hostname;
    } catch (err) {
        logger.error(err, `Failed to parse domain from URL: ${url}`);
        return 'unknown';
    }
}
