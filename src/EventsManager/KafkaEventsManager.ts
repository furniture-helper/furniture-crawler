import { Kafka, KafkaConfig, Producer } from 'kafkajs';
import EventsManager, { EventCategory, EventData, EventMetadata } from './EventsManager';
import Logger from '../Logger';

const KAFKA_BROKER: string = process.env.KAFKA_BROKER!;

export default class KafkaEventsManager extends EventsManager {
    private readonly producer: Producer;
    private connectPromise: Promise<void> | null = null;

    constructor() {
        super();
        const kafkaConfig: KafkaConfig = {
            clientId: 'crawler',
            brokers: [KAFKA_BROKER],
        };
        const kafka = new Kafka(kafkaConfig);
        this.producer = kafka.producer();
    }

    protected async publish(event: EventData, metadata: EventMetadata, category: EventCategory): Promise<void> {
        await this.connect();
        const topic = await this.getKafkaTopicFromEventCategory(category);
        await this.producer.send({
            topic: topic,
            messages: [
                {
                    value: JSON.stringify(event),
                    headers: {
                        host: metadata.host,
                        region: metadata.region,
                    },
                },
            ],
        });
        Logger.debug(`Published event to Kafka topic ${topic}: ${JSON.stringify(event)}`);
    }

    private async connect(): Promise<void> {
        if (!this.connectPromise) {
            this.connectPromise = this.producer.connect();
        }
        await this.connectPromise;
    }

    private async getKafkaTopicFromEventCategory(category: EventCategory): Promise<string> {
        switch (category) {
            case EventCategory.CRAWL:
                return process.env.CRAWLER_EVENTS_TOPIC!;
            case EventCategory.CLASSIFICATION:
                return process.env.CLASSIFICATION_EVENTS_TOPIC!;
            case EventCategory.EXTRACTION:
                return process.env.EXTRACTION_EVENTS_TOPIC!;
            default:
                throw new Error(`Unknown event category: ${category}`);
        }
    }
}
