import { getDomainFromUrl } from '../utils/url_utils';

export enum CrawlerEventStatus {
    SUCCESS = 'success',
    FAILURE = 'failure',
}

export type CrawlEventData = {
    url: string;
    domain: string;
    duration: number;
    status: CrawlerEventStatus;
    statusCode: number;
    error?: string | undefined;
};

export type CrawlEventMetadata = {
    host: string;
};

export default abstract class EventsManager {
    private host: string | undefined;

    public async pushEvent(
        url: string,
        duration: number,
        status: CrawlerEventStatus,
        statusCode: number,
        error?: string | undefined,
    ): Promise<void> {
        const event: CrawlEventData = {
            url: url,
            domain: getDomainFromUrl(url),
            duration: duration,
            status: status,
            statusCode: statusCode,
            error: error,
        };

        const metadata = {
            host: await this.getHost(),
        };

        await this.publish(event, metadata);
    }

    protected abstract publish(event: CrawlEventData, metadata: CrawlEventMetadata): Promise<void>;

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
