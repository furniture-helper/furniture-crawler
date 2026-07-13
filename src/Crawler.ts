import { Configuration, log, Log, PlaywrightCrawler, PlaywrightCrawlingContext } from 'crawlee';
import { execFile } from 'node:child_process';
import { existsSync, readdirSync, statSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';
import {
    getMaxConcurrency,
    getMaxRequestsPerCrawl,
    getMaxRequestsPerMinute,
    getNavigationTimeoutSecs,
    getPageStorageConstructor,
    getRequestHandlerTimeoutSecs,
} from './config';
import { launchOptions } from 'camoufox-js';

import { getSpecialization } from './Specializations/Specialization';
import logger from './Logger';
import DatabaseUpsertQueue from './db/DBUpsertQueue';
import {
    addNewUrls,
    blockAds,
    blockIframes,
    blockUnnecessaryResources,
    checkForBlackListedUrl,
    checkForRedirect,
    isUselessPage,
    removeCommonElements,
    resolveToAbsoluteUrls,
    waitForDomContentLoaded,
} from './utils/crawler_utils';
import { Queue } from './CrawlerQueue/Queue';
import { getDomainFromUrl } from './utils/url_utils';
import { AbortedRequestError } from './errors';
import { extract_details_from_jsonld_schema } from './utils/json_ld_extraction_utils';
import { firefox } from 'playwright';

Configuration.set('systemInfoV2', true);
Configuration.set('availableMemoryRatio', 0.8);
Configuration.set('maxUsedCpuRatio', 0.8);
Configuration.set('containerized', true);

const execFileAsync = promisify(execFile);
const CAMOUFOX_EXECUTABLE_CANDIDATES = ['camoufox-bin', 'camoufox', 'firefox', 'firefox-bin', 'camoufox.exe'];
const NPX_EXECUTABLE = process.platform === 'win32' ? 'npx.cmd' : 'npx';

export default class Crawler {
    private crawler!: PlaywrightCrawler;
    private readonly settings = {
        headless: true,
        maxRequestsPerCrawl: getMaxRequestsPerCrawl(),
        maxConcurrency: getMaxConcurrency(),
        maxRequestsPerMinute: getMaxRequestsPerMinute(),
        autoscaledPoolOptions: {
            desiredConcurrencyRatio: 0.8,
            maxConcurrency: getMaxConcurrency(),
        },
        requestHandlerTimeoutSecs: getRequestHandlerTimeoutSecs(),
        persistCookiesPerSession: true,
        navigationTimeoutSecs: getNavigationTimeoutSecs(),
        maxRequestRetries: 3,
    };

    private readonly pageStorageConstructor = getPageStorageConstructor();
    private readonly backoffDomains: Map<string, Date> = new Map();

    private readonly crawlerLog = new Log({
        level: log.LEVELS.INFO,
    });

    private constructor() {
        const originalError = this.crawlerLog.error.bind(this.crawlerLog);

        this.crawlerLog.error = (message?: unknown, ...optionalParams: unknown[]) => {
            const text = [message, ...optionalParams]
                .map((v) => (typeof v === 'string' ? v : v instanceof Error ? v.message : JSON.stringify(v)))
                .join(' ');

            // Suppress only this control-flow failure noise
            if (
                text.includes('AbortedRequestError') ||
                text.includes('Aborted request for ') ||
                text.includes('received 403 status code') ||
                text.includes('received 429 status code')
            ) {
                return;
            }

            originalError(message as any);
        };
    }

    public static async create(): Promise<Crawler> {
        const launchOptionsConfig = await Crawler.getCamoufoxLaunchOptions();

        const instance = new Crawler();

        instance.crawler = new PlaywrightCrawler({
            ...instance.settings,
            log: instance.crawlerLog,
            preNavigationHooks: [
                checkForBlackListedUrl.bind(instance),
                instance.isInIgnoredDomain.bind(instance),
                waitForDomContentLoaded.bind(instance),
                blockAds.bind(instance),
                blockIframes.bind(instance),
                blockUnnecessaryResources.bind(instance),
            ],
            postNavigationHooks: [
                async ({ handleCloudflareChallenge }) => {
                    await handleCloudflareChallenge();
                },
                checkForRedirect.bind(instance),
            ],
            requestHandler: instance.requestHandler.bind(instance),
            failedRequestHandler: instance.failedRequestHandler.bind(instance),
            errorHandler: instance.errorHandler.bind(instance),
            browserPoolOptions: {
                useFingerprints: false,
            },
            launchContext: {
                launcher: firefox,
                launchOptions: launchOptionsConfig,
            },
        });

        return instance;
    }

    private static getCamoufoxInstallDir(): string {
        return process.env.CAMOUFOX_INSTALL_DIR || path.join(os.homedir(), '.cache', 'camoufox');
    }

    private static async getCamoufoxCliInstallDir(): Promise<string | null> {
        try {
            const { stdout } = await execFileAsync(NPX_EXECUTABLE, ['camoufox-js', 'path'], {
                env: process.env,
            });
            const resolved = stdout
                .split(/\r?\n/)
                .map((line) => line.trim())
                .find((line) => line.length > 0);
            return resolved || null;
        } catch {
            return null;
        }
    }

    private static findCamoufoxExecutable(dir: string): string | null {
        if (!existsSync(dir)) {
            return null;
        }

        const stack = [dir];

        while (stack.length > 0) {
            const current = stack.pop();
            if (!current) continue;

            for (const entry of readdirSync(current)) {
                const fullPath = path.join(current, entry);
                const stats = statSync(fullPath);

                if (stats.isDirectory()) {
                    stack.push(fullPath);
                    continue;
                }

                if (CAMOUFOX_EXECUTABLE_CANDIDATES.includes(entry)) {
                    return fullPath;
                }
            }
        }

        return null;
    }

    private static async buildCamoufoxLaunchOptions() {
        const installDirs = [
            Crawler.getCamoufoxInstallDir(),
            path.join(os.homedir(), '.cache', 'camoufox'),
            '/root/.cache/camoufox',
            '/tmp/camoufox',
        ];
        const cliInstallDir = await Crawler.getCamoufoxCliInstallDir();
        if (cliInstallDir) {
            installDirs.unshift(cliInstallDir);
        }

        const executablePath = [...new Set(installDirs)]
            .map((dir) => Crawler.findCamoufoxExecutable(dir))
            .find((value): value is string => Boolean(value));

        return await launchOptions({
            headless: true,
            ...(executablePath ? { executable_path: executablePath } : {}),
        });
    }

    private static async getCamoufoxLaunchOptions() {
        try {
            return await Crawler.buildCamoufoxLaunchOptions();
        } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            const needsInstall =
                message.includes('CamoufoxNotInstalled') ||
                message.includes('Version information not found') ||
                message.includes('Please run `camoufox fetch` to install.');

            if (!needsInstall) {
                throw err;
            }

            logger.warn(`Camoufox browser payload missing; fetching it now.`);
            await execFileAsync(NPX_EXECUTABLE, ['camoufox-js', 'fetch'], {
                env: {
                    ...process.env,
                    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD: '0',
                },
            });

            return await Crawler.buildCamoufoxLaunchOptions();
        }
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

    private async requestHandler({ request, page }: PlaywrightCrawlingContext): Promise<void> {
        if (request.userData?.isDownload) {
            await this.removeFromQueueAndSetInactive(request.url);
            return;
        }

        if (request.skipNavigation) {
            return;
        }

        if (!request.loadedUrl) {
            logger.error(`No loaded URL for request: ${request.url}`);
            await this.removeFromQueueAndSetInactive(request.url);
            return;
        }

        const startTime = Date.now();

        logger.debug(`Parsing page: ${request.loadedUrl}`);

        // Abort loading of unnecessary resources to speed up page load
        await page.route('**/*.{png,jpg,jpeg,gif,css,woff}', (route) => route.abort());

        await page.waitForLoadState('load');

        // wait for network to be idle (or timeout after 5 seconds)
        await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {
            logger.warn(`Network idle timeout for ${request.loadedUrl}`);
        });

        logger.debug(`Page loaded: ${request.loadedUrl} in ${Date.now() - startTime} ms`);

        // Check if the page is considered "useless" and should not be crawled
        if (await isUselessPage(request.loadedUrl, page)) {
            logger.debug(`Skipping useless page: ${request.loadedUrl}`);
            await this.removeFromQueueAndSetInactive(request.url);
            return;
        }

        // A specialization is a set of custom actions that will be applied to a page from a specific website.
        // For example, hiding pop-ups, closing modals, or any other action that improves data extraction.
        const specialization = await getSpecialization(request.loadedUrl, page);
        if (specialization) {
            logger.debug(`Resolving specialization for ${request.loadedUrl}`);
            await specialization.apply();
        }

        await resolveToAbsoluteUrls(page);
        logger.debug(`Resolved relative URLs to absolute for page: ${request.loadedUrl}`);

        await removeCommonElements(page);

        // Store the page using the selected storage mechanism
        logger.debug(`Working on storing page: ${request.loadedUrl}`);
        const storage = new this.pageStorageConstructor(request.loadedUrl, page);
        await storage.store();

        await Queue.deleteMessage(request.url);
        logger.debug(`Completed processing for page: ${request.loadedUrl}`);

        await addNewUrls(request.loadedUrl, page).catch((err) => {
            logger.error(err, `Error adding new URLs from page: ${request.loadedUrl}`);
        });

        await extract_details_from_jsonld_schema(request.loadedUrl, page).catch((err) => {
            logger.error(err, `Error extracting JSON-LD details from page: ${request.loadedUrl}`);
        });
    }

    private async failedRequestHandler({ request, error }: PlaywrightCrawlingContext): Promise<void> {
        if (error instanceof AbortedRequestError) {
            return;
        }

        logger.error(error, `Request failed for ${request.url}`);

        if (
            error instanceof Error &&
            (error.message.includes('received 429 status code') || error.message.includes('received 403 status code'))
        ) {
            const domain = getDomainFromUrl(request.url);
            this.backoffDomains.set(domain, new Date());
        }
    }

    private async errorHandler({ request, error }: PlaywrightCrawlingContext): Promise<void> {
        if (error instanceof AbortedRequestError) {
            return;
        }

        if (
            error instanceof Error &&
            (error.message.includes('received 429 status code') || error.message.includes('received 403 status code'))
        ) {
            request.noRetry = true;
            const domain = getDomainFromUrl(request.url);
            this.backoffDomains.set(domain, new Date());
        }
    }

    private async removeFromQueueAndSetInactive(url: string): Promise<void> {
        await DatabaseUpsertQueue.setInactive(url);
        await Queue.deleteMessage(url);
    }

    private isInIgnoredDomain({ request }: PlaywrightCrawlingContext) {
        const domain = getDomainFromUrl(request.url);
        if (this.backoffDomains.has(domain)) {
            const backOffStart = this.backoffDomains.get(domain)!;
            const backOffDuration = 60 * 1000; // 1 minute backoff
            const timeSinceBackoff = Date.now() - backOffStart.getTime();
            if (timeSinceBackoff < backOffDuration) {
                logger.info(
                    `Backing off from domain ${domain} for ${Math.ceil((backOffDuration - timeSinceBackoff) / 1000)} seconds`,
                );
                request.noRetry = true;
                request.skipNavigation = true;
                throw new AbortedRequestError(request.url);
            } else {
                this.backoffDomains.delete(domain);
            }
        }
    }
}
