import pino from 'pino';

const logger = pino({
    level: process.env.LOG_LEVEL || 'info',
    base: null,
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: {
        level(label: string) {
            return { level: label.toUpperCase() };
        },
    },
});

export default logger;
