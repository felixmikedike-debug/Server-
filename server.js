'use strict';

require('dotenv').config();

const path = require('path');
const Fastify = require('fastify');
const mongoose = require('mongoose');

// ==================== VALIDATE REQUIRED ENV VARS ====================
const REQUIRED_ENV = ['DOMAIN'];
for (const key of REQUIRED_ENV) {
  if (!process.env[key]) {
    console.error(`FATAL: Missing required environment variable: ${key}`);
    process.exit(1);
  }
}

const PORT = parseInt(process.env.PORT, 10) || 3000;

// ==================== BUILD APP ====================
const app = Fastify({
  trustProxy: true,
  logger: {
    level: process.env.LOG_LEVEL || 'info',
    transport: process.env.NODE_ENV !== 'production'
      ? { target: 'pino-pretty', options: { colorize: true } }
      : undefined
  },
  ajv: {
    customOptions: {
      removeAdditional: 'all',   // strip unknown fields
      useDefaults: true,
      coerceTypes: true,
      allErrors: false
    }
  }
});

// ==================== REGISTER PLUGINS ====================
async function registerPlugins() {
  // CORS
  await app.register(require('@fastify/cors'), {
    origin: true,
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS']
  });

  // Static files
  await app.register(require('@fastify/static'), {
    root: path.join(__dirname, 'public'),
    prefix: '/public/'
  });

  // View engine (EJS)
  await app.register(require('@fastify/view'), {
    engine: { ejs: require('ejs') },
    root: path.join(__dirname, 'views'),
    propertyName: 'view'
  });

  // Form body parsing
  await app.register(require('@fastify/formbody'));

  // Rate limiting
  await app.register(require('@fastify/rate-limit'), {
    global: false,
    redis: require('./src/redis').redisConnection
  });
}

// ==================== REGISTER ROUTES ====================
async function registerRoutes() {
  await app.register(require('./src/routes/auth'),         { prefix: '/api/auth' });
  await app.register(require('./src/routes/subscription'), { prefix: '/api/subscription' });
  await app.register(require('./src/routes/pages'),        { prefix: '/api' });
  await app.register(require('./src/routes/forms'),        { prefix: '/api' });
  await app.register(require('./src/routes/contacts'),     { prefix: '/api' });
  await app.register(require('./src/routes/broadcast'),    { prefix: '/api/broadcast' });
  await app.register(require('./src/routes/webhook'));
  await app.register(require('./src/routes/public'));
  await app.register(require('./src/routes/admin'));
}

// ==================== GLOBAL ERROR HANDLER ====================
app.setErrorHandler((err, req, reply) => {
  // Validation errors from AJV
  if (err.validation) {
    return reply.status(400).send({
      error: 'Validation failed',
      details: err.validation.map(v => v.message)
    });
  }

  // Known HTTP errors
  if (err.statusCode) {
    return reply.status(err.statusCode).send({ error: err.message });
  }

  req.log.error({ err }, 'Unhandled error');
  reply.status(500).send({ error: 'Internal server error' });
});

// 404 handler
app.setNotFoundHandler((req, reply) => {
  if (req.url.startsWith('/api/')) {
    return reply.status(404).send({ error: 'Not found' });
  }
  reply.status(404).view('404');
});

// ==================== HEALTH CHECK ====================
app.get('/ping', { logLevel: 'silent' }, (req, reply) => {
  reply.status(200).type('text/plain').send('ok');
});

// ==================== STARTUP ====================
async function start() {
  try {
    const { MONGODB_URI } = require('./src/config');
    app.log.info(`Connecting to MongoDB: ${MONGODB_URI.replace(/:([^:@]+)@/, ':****@')}`);

    await mongoose.connect(MONGODB_URI, {
      serverSelectionTimeoutMS: 30000,
      socketTimeoutMS: 45000,
      connectTimeoutMS: 30000
    });
    app.log.info('✅ MongoDB connected');

    // Load admin settings, launch bots, recover broadcasts
    const { loadAdminSettings } = require('./src/adminSettings');
    const { launchAllBots }     = require('./src/bots');
    const { recoverScheduled }  = require('./src/worker');

    await loadAdminSettings();
    await launchAllBots();
    await recoverScheduled();

    await registerPlugins();
    await registerRoutes();

    await app.listen({ port: PORT, host: '0.0.0.0' });
    app.log.info(`\n🚀 SENDM FASTIFY SERVER\nPort: ${PORT} | Domain: https://${process.env.DOMAIN}\n`);
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

// ==================== GRACEFUL SHUTDOWN ====================
async function shutdown(signal) {
  app.log.info(`${signal} received — shutting down gracefully`);
  try {
    const { worker, broadcastQueue } = require('./src/worker');
    await worker.close();
    await broadcastQueue.close();
    await mongoose.connection.close();
    await app.close();
    app.log.info('Shutdown complete');
    process.exit(0);
  } catch (err) {
    app.log.error(err, 'Error during shutdown');
    process.exit(1);
  }
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));

process.on('unhandledRejection', (reason) => {
  app.log.error({ reason }, 'Unhandled promise rejection');
});

start();
