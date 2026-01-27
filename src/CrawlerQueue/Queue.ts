import { DeleteMessageCommand, ReceiveMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import logger from '../Logger';
import { getMaxRequestsPerCrawl } from '../config';

export type Message = {
    url: string;
    receiptHandle: string;
};

export class Queue {
    private static receiptHandles: Map<string, string> = new Map();
    private static client: SQSClient;
    private static sqsUrl: string = '';

    public static init() {
        const sqsUrl = process.env.SQS_QUEUE_URL;
        if (!sqsUrl) {
            throw new Error('SQS_QUEUE_URL environment variable is not set');
        }

        this.sqsUrl = sqsUrl;
        this.client = new SQSClient({ region: process.env.AWS_REGION ?? 'eu-west-1' });
    }

    public static async getMessages(): Promise<Message[]> {
        logger.debug('Getting messages from SQS queue');

        const desired = Math.ceil(getMaxRequestsPerCrawl() / 10);
        const maxNumberOfMessages = Math.min(10, Math.max(1, desired));
        const messages = await this.client.send(
            new ReceiveMessageCommand({
                QueueUrl: this.sqsUrl,
                MaxNumberOfMessages: maxNumberOfMessages,
                WaitTimeSeconds: 20,
            }),
        );
        if (!messages.Messages || messages.Messages.length === 0) {
            return [];
        }

        const result: Message[] = [];
        for (const message of messages.Messages) {
            if (message.Body && message.ReceiptHandle) {
                result.push({
                    url: message.Body,
                    receiptHandle: message.ReceiptHandle,
                });
                Queue.receiptHandles.set(message.Body, message.ReceiptHandle);
            }
        }

        logger.debug(`Received ${result.length} messages from SQS`);
        return result;
    }

    public static async deleteMessage(url: string): Promise<void> {
        logger.debug(`Deleting ${url} from SQS queue`);

        const receiptHandle = this.receiptHandles.get(url);
        if (!receiptHandle) {
            logger.warn(`Receipt Handle ${url} not found.`);
            return;
        }
        logger.debug(`Deleting message with receipt handle: ${receiptHandle}`);
        await this.client.send(
            new DeleteMessageCommand({
                QueueUrl: this.sqsUrl,
                ReceiptHandle: receiptHandle,
            }),
        );
        logger.debug(`Deleted message with receiptHandle: ${receiptHandle}`);
    }
}
