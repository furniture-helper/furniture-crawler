import { getDomainFromUrl } from '../utils/url_utils';
import Logger from '../Logger';

export enum CrawlerEventStatus {
    SUCCESS = 'success',
    FAILURE = 'failure',
}

export interface EventData {
    url: string;
    domain: string;
}

export type CrawlEventData = EventData & {
    duration: number;
    status: CrawlerEventStatus;
    error?: string | undefined;
};

export type ClassificationEventData = EventData & {
    classification: string;
    confidence: number;
    source: string;
};

export type ExtractionEventData = EventData & {
    productTitle: string;
    productPrice: number;
    productImageUrl: string;
    inStock: boolean;
    source: string;
};

export enum EventCategory {
    CRAWL,
    CLASSIFICATION,
    EXTRACTION,
}

export type EventMetadata = {
    host: string;
    region: string;
};

export default abstract class EventsManager {
    private host: string | undefined;

    public async pushCrawlEvent(
        url: string,
        duration: number,
        status: CrawlerEventStatus,
        error?: string | undefined,
    ): Promise<void> {
        const event: CrawlEventData = {
            url: url,
            domain: getDomainFromUrl(url),
            duration: duration,
            status: status,
            error: error,
        };

        await this.publishEventWithMetadata(event, EventCategory.CRAWL);
    }

    public async pushClassificationEvent(url: string, classification: string): Promise<void> {
        const event: ClassificationEventData = {
            url: url,
            domain: getDomainFromUrl(url),
            classification: classification,
            confidence: 1.0,
            source: 'ecs_crawler',
        };

        await this.publishEventWithMetadata(event, EventCategory.CLASSIFICATION);
    }

    public async pushExtractionEvent(
        url: string,
        productTitle: string,
        productPrice: number,
        productImageUrl: string,
        inStock: boolean,
    ): Promise<void> {
        const event: ExtractionEventData = {
            url: url,
            domain: getDomainFromUrl(url),
            productTitle: productTitle,
            productPrice: productPrice,
            productImageUrl: productImageUrl,
            inStock: inStock,
            source: 'ecs_crawler',
        };

        await this.publishEventWithMetadata(event, EventCategory.EXTRACTION);
    }

    protected abstract publish(event: EventData, metadata: EventMetadata, category: EventCategory): Promise<void>;

    private async publishEventWithMetadata(event: EventData, category: EventCategory): Promise<void> {
        const metadata = {
            host: await this.getHost(),
            region: await this.getRegion(),
        };

        try {
            await this.publish(event, metadata, category);
        } catch (error) {
            Logger.error(
                `Failed to publish event: ${error instanceof Error ? error.stack || error.message : String(error)}`,
            );
        }
    }

    private async getHost(): Promise<string> {
        if (this.host !== undefined) {
            return this.host;
        }

        this.host = process.env.RUNNING_MODE;
        if (process.env.RUNNING_MODE === 'ecs') {
            const ecsTaskId = await this.getEcsTaskId();
            if (ecsTaskId) {
                this.host = ecsTaskId;
            }
        }

        return this.host || 'unknown';
    }

    private async getRegion(): Promise<string> {
        return process.env.AWS_REGION || 'unknown';
    }

    private async getEcsTaskId(): Promise<string | undefined> {
        const metadataUri = process.env.ECS_CONTAINER_METADATA_URI_V4;
        if (!metadataUri) return undefined;

        const res = await fetch(`${metadataUri}/task`);
        if (!res.ok) {
            throw new Error(`Failed to read ECS task metadata: ${res.status} ${res.statusText}`);
        }

        const data = (await res.json()) as { TaskARN?: string };
        return data.TaskARN?.split('/').pop();
    }
}
