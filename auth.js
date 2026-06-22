'use strict';

const fp  = require('fastify-plugin');
const jwt = require('jsonwebtoken');
const { JWT_SECRET } = require('../config');
const { User }       = require('../models');

/**
 * Adds `fastify.authenticate` preHandler hook.
 * Sets req.user to the authenticated Mongoose User document.
 */
module.exports = fp(async function authPlugin(fastify) {
  fastify.decorate('authenticate', async function (req, reply) {
    const authHeader = req.headers.authorization;
    const token = authHeader?.startsWith('Bearer ')
      ? authHeader.slice(7)
      : req.query?.token;

    if (!token) {
      return reply.status(401).send({ error: 'Access token required' });
    }

    let decoded;
    try {
      decoded = jwt.verify(token, JWT_SECRET);
    } catch {
      return reply.status(403).send({ error: 'Invalid or expired token' });
    }

    const user = await User.findOne({ id: decoded.userId });
    if (!user) {
      return reply.status(404).send({ error: 'User not found' });
    }

    req.user = user;
  });
});
