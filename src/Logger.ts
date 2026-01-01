import pino from 'pino';

const logger = pino({
    level: process.env.LOG_LEVEL || 'info',
    base: null,
    timestamp: () => `,"time":"${new Date().toISOString().slice(0, 19).replace('T', ' ')}"`,
    formatters: {
        level(label: string) {
            return { level: label.toUpperCase() };
        },
    },
});

export default logger;
