import Logger from '../Logger';
import { Kafka, KafkaConfig, Producer } from 'kafkajs';
import EventsManager, { CrawlEventData, CrawlEventMetadata } from './EventsManager';

const CRAWLER_EVENTS_TOPIC: string = process.env.CRAWLER_EVENTS_TOPIC!;
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

    protected async publish(event: CrawlEventData, metadata: CrawlEventMetadata): Promise<void> {
        await this.connect();
        const result = await this.producer.send({
            topic: CRAWLER_EVENTS_TOPIC,
            messages: [
                {
                    value: JSON.stringify(event),
                    headers: {
                        host: metadata.host,
                    },
                },
            ],
        });
        Logger.info(`Event sent to Kafka topic ${CRAWLER_EVENTS_TOPIC} with result: ${JSON.stringify(result)}`);
    }

    private async connect(): Promise<void> {
        if (!this.connectPromise) {
            this.connectPromise = this.producer.connect();
        }
        await this.connectPromise;
    }
}
