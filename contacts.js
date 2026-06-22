'use strict';

const fp = require('fastify-plugin');

const { Contact }            = require('../models');
const { getUserCache, invalidateUserCache } = require('../cache');

const deleteSchema = {
  body: {
    type: 'object',
    required: ['contacts'],
    properties: {
      contacts: {
        type: 'array',
        minItems: 1,
        maxItems: 500,
        items: { type: 'string', minLength: 1 }
      }
    }
  }
};

module.exports = fp(async function contactsRoutes(fastify) {
  await fastify.register(require('../middleware/auth'));

  // GET /api/contacts
  fastify.get('/contacts', { preHandler: fastify.authenticate }, async (req, reply) => {
    const bucket = getUserCache(req.user.id);
    const TTL    = 2 * 60 * 1000;
    const now    = Date.now();

    if (bucket.contacts && now - bucket.contactsTs < TTL) {
      return reply.send({ success: true, contacts: bucket.contacts });
    }

    const contacts = await Contact.find({ userId: req.user.id })
      .sort({ submittedAt: -1 })
      .lean();

    const formatted = contacts.map(c => ({
      name:           c.name,
      contact:        c.contact,
      status:         c.status,
      telegramChatId: c.telegramChatId || null,
      pageId:         c.shortId,
      submittedAt:    c.submittedAt ? new Date(c.submittedAt).toLocaleString() : null,
      subscribedAt:   c.subscribedAt ? new Date(c.subscribedAt).toLocaleString() : null
    }));

    bucket.contacts  = formatted;
    bucket.contactsTs = now;
    return reply.send({ success: true, contacts: formatted });
  });

  // POST /api/contacts/delete
  fastify.post('/contacts/delete', { schema: deleteSchema, preHandler: fastify.authenticate }, async (req, reply) => {
    const result = await Contact.deleteMany({
      userId:  req.user.id,
      contact: { $in: req.body.contacts }
    });

    invalidateUserCache(req.user.id, 'contacts');
    return reply.send({ success: true, deletedCount: result.deletedCount });
  });
});
