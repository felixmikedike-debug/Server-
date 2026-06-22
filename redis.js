'use strict';

const IORedis = require('ioredis');

let redisConnection;

if (process.env.REDIS_URL) {
  redisConnection = new IORedis(process.env.REDIS_URL, {
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
    lazyConnect: true
  });
} else {
  console.warn('⚠️  WARNING: REDIS_URL not set — falling back to localhost:6379');
  redisConnection = new IORedis({
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT, 10) || 6379,
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
    lazyConnect: true
  });
}

redisConnection.on('error', (err) => {
  console.error('Redis error:', err.message);
});

redisConnection.on('connect', () => {
  console.log('✅ Redis connected');
});

module.exports = { redisConnection };
