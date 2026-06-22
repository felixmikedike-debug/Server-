'use strict';

const fp = require('fastify-plugin');
const { WEBHOOK_SECRET } = require('../config');
const { activeBots }     = require('../bots');

module.exports = fp(async function webhookRoute(fastify) {
  /**
   * POST /webhook/:secret/:userId
   * Receives updates from Telegram and dispatches them to the correct bot.
   * Uses the secret token embedded in the URL path for lightweight auth.
   */
  fastify.post(
    `/webhook/${WEBHOOK_SECRET}/:userId`,
    { logLevel: 'warn' },   // reduce noise from high-volume webhook traffic
    async (req, reply) => {
      const { userId } = req.params;

      // Parse body — Fastify with express-compatible raw body or JSON plugin
      let update;
      try {
        if (Buffer.isBuffer(req.body)) {
          update = JSON.parse(req.body.toString('utf8'));
        } else if (req.body && typeof req.body === 'object') {
          update = req.body;
        } else {
          return reply.status(400).send('Bad Request');
        }
      } catch {
        return reply.status(400).send('Malformed JSON');
      }

      const bot = activeBots.get(userId);
      if (bot) {
        try {
          await bot.handleUpdate(update);
        } catch (err) {
          // Never let internal bot errors bubble to Telegram (it would retry)
          req.log.error({ err, userId }, 'Bot update handler error');
        }
      } else {
        req.log.warn({ userId }, 'Received webhook for unknown/inactive bot');
      }

      // Always 200 — Telegram will retry on non-2xx
      return reply.status(200).send('OK');
    }
  );
});
