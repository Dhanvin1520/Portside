import winston from 'winston';

// ─────────────────────────────────────────────────────────────────────────────
// Structured Logger
// Consistent logging across all Portside components
// ─────────────────────────────────────────────────────────────────────────────

export function createLogger(component: string): winston.Logger {
  return winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: winston.format.combine(
      winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
      winston.format.errors({ stack: true }),
      winston.format.printf(({ timestamp, level, message, component: comp, ...meta }) => {
        const metaStr = Object.keys(meta).length > 0
          ? ` ${JSON.stringify(meta)}`
          : '';
        return `${timestamp} [${level.toUpperCase().padEnd(5)}] [${comp || component}] ${message}${metaStr}`;
      })
    ),
    defaultMeta: { component },
    transports: [
      new winston.transports.Console(),
    ],
  });
}
