'use strict';

const fp             = require('fastify-plugin');
const { v4: uuidv4 } = require('uuid');

const { FormPage, User, Contact }  = require('../models');
const { getUserLimits, sanitizeTelegramHtml } = require('../utils');
const { adminSettingsCache }  = require('../adminSettings');
const { pendingSubscribers }  = require('../bots');
const { CONTACT_REGEX }       = require('../config');
const {
  getUserCache, invalidateUserCache,
  getPublicCache, setPublicCache, invalidatePublicCache
} = require('../cache');

// ==================== SCHEMAS ====================
const saveSchema = {
  body: {
    type: 'object',
    required: ['title', 'state'],
    properties: {
      shortId:        { type: 'string', maxLength: 50 },
      title:          { type: 'string', minLength: 1, maxLength: 200 },
      state:          { type: 'object' },
      welcomeMessage: { type: 'string', maxLength: 2000, default: '' }
    }
  }
};

const deleteSchema = {
  body: {
    type: 'object',
    required: ['shortId'],
    properties: {
      shortId: { type: 'string', minLength: 1, maxLength: 50 }
    }
  }
};

const subscribeSchema = {
  body: {
    type: 'object',
    required: ['name', 'email'],
    properties: {
      name:  { type: 'string', minLength: 1, maxLength: 120 },
      email: { type: 'string', minLength: 1, maxLength: 300 }   // "email" field = contact (email or phone)
    }
  }
};

// ==================== PLUGIN ====================
module.exports = fp(async function formsRoutes(fastify) {
  await fastify.register(require('../middleware/auth'));

  // GET /api/forms
  fastify.get('/forms', { preHandler: fastify.authenticate }, async (req, reply) => {
    const bucket = getUserCache(req.user.id);
    const TTL    = 5 * 60 * 1000;
    const now    = Date.now();

    if (bucket.forms && now - bucket.formsTs < TTL) {
      return reply.send({ forms: bucket.forms });
    }

    const forms = await FormPage.find({ userId: req.user.id }).sort({ updatedAt: -1 }).lean();
    const base  = `${req.protocol}://${req.hostname}`;

    const formatted = forms.map(f => ({
      shortId:   f.shortId,
      title:     f.title,
      createdAt: f.createdAt,
      updatedAt: f.updatedAt,
      url:       `${base}/f/${f.shortId}`
    }));

    bucket.forms  = formatted;
    bucket.formsTs = now;
    return reply.send({ forms: formatted });
  });

  // GET /api/form/:shortId (public)
  fastify.get('/form/:shortId', async (req, reply) => {
    const { shortId } = req.params;
    const cacheKey    = `apiForm:${shortId}`;
    const cached      = getPublicCache(cacheKey);
    if (cached) return reply.send(cached);

    const form = await FormPage.findOne({ shortId }).lean();
    if (!form) return reply.status(404).send({ error: 'Form not found' });

    const data = { shortId: form.shortId, title: form.title, state: form.state, welcomeMessage: form.welcomeMessage };
    setPublicCache(cacheKey, data);
    return reply.send(data);
  });

  // POST /api/forms/save
  fastify.post('/forms/save', { schema: saveSchema, preHandler: fastify.authenticate }, async (req, reply) => {
    const { shortId, title, state, welcomeMessage } = req.body;
    const limits = getUserLimits(req.user, adminSettingsCache);

    if (!shortId) {
      const count = await FormPage.countDocuments({ userId: req.user.id });
      if (limits.maxForms !== Infinity && count >= limits.maxForms) {
        return reply.status(403).send({ error: `Free plan allows up to ${limits.maxForms} forms.` });
      }
    } else {
      const existing = await FormPage.findOne({ shortId }).lean();
      if (existing && existing.userId !== req.user.id) {
        return reply.status(403).send({ error: 'You do not own this form.' });
      }
    }

    // Sanitize state fields
    const sanitizedState = JSON.parse(JSON.stringify(state));
    const scriptRe = /<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi;
    if (sanitizedState.headerText)    sanitizedState.headerText    = sanitizedState.headerText.replace(scriptRe, '');
    if (sanitizedState.subheaderText) sanitizedState.subheaderText = sanitizedState.subheaderText.replace(scriptRe, '');
    if (sanitizedState.buttonText)    sanitizedState.buttonText    = sanitizedState.buttonText.replace(scriptRe, '');

    const sanitizedWelcome = sanitizeTelegramHtml(welcomeMessage || '');

    const finalId = shortId || uuidv4().slice(0, 8);
    const now     = new Date();

    await FormPage.findOneAndUpdate(
      { shortId: finalId },
      {
        $set:         { userId: req.user.id, title: title.trim(), state: sanitizedState, welcomeMessage: sanitizedWelcome, updatedAt: now },
        $setOnInsert: { createdAt: now }
      },
      { upsert: true }
    );

    invalidateUserCache(req.user.id, 'forms');
    invalidatePublicCache(`form:${finalId}`);
    invalidatePublicCache(`apiForm:${finalId}`);

    return reply.send({
      success: true,
      shortId: finalId,
      url:     `${req.protocol}://${req.hostname}/f/${finalId}`
    });
  });

  // POST /api/forms/delete
  fastify.post('/forms/delete', { schema: deleteSchema, preHandler: fastify.authenticate }, async (req, reply) => {
    const { shortId } = req.body;
    const form = await FormPage.findOne({ shortId, userId: req.user.id });
    if (!form) return reply.status(404).send({ error: 'Form not found.' });

    await FormPage.deleteOne({ shortId });
    await Contact.deleteMany({ shortId, userId: req.user.id });

    invalidateUserCache(req.user.id, 'forms');
    invalidatePublicCache(`form:${shortId}`);
    invalidatePublicCache(`apiForm:${shortId}`);

    return reply.send({ success: true });
  });

  // POST /api/subscribe/:shortId (public, rate-limited)
  fastify.post('/subscribe/:shortId', {
    schema: subscribeSchema,
    config: {
      rateLimit: {
        max: 10,
        timeWindow: '15 minutes',
        keyGenerator: (req) => `${req.ip}::${req.params.shortId}`
      }
    }
  }, async (req, reply) => {
    const { shortId }    = req.params;
    const name           = req.body.name.trim();
    const contactValue   = req.body.email.trim();

    if (!CONTACT_REGEX.test(contactValue)) {
      return reply.status(400).send({ error: 'Contact must be a valid email address or phone number.' });
    }

    const form = await FormPage.findOne({ shortId }).lean();
    if (!form) return reply.status(404).send({ error: 'Form not found.' });

    const owner = await User.findOne({ id: form.userId }).lean();
    if (!owner?.telegramBotToken || !owner?.botUsername) {
      return reply.status(400).send({ error: 'This form is not connected to a Telegram bot yet.' });
    }

    const payload = `sub_${shortId}_${uuidv4().slice(0, 12)}`;

    let contact = await Contact.findOne({ userId: owner.id, contact: contactValue });

    if (contact) {
      contact.name      = name;
      contact.shortId   = shortId;
      contact.submittedAt = new Date();
    } else {
      contact = new Contact({
        userId:      owner.id,
        shortId,
        name,
        contact:     contactValue,
        status:      'pending',
        submittedAt: new Date()
      });
    }
    await contact.save();

    pendingSubscribers.set(payload, {
      userId:    owner.id,
      shortId,
      name,
      contact:   contactValue,
      createdAt: Date.now()
    });

    invalidateUserCache(owner.id, 'contacts');

    return reply.send({
      success:          true,
      deepLink:         `https://t.me/${owner.botUsername}?start=${payload}`,
      alreadySubscribed: contact.status === 'subscribed'
    });
  });
});
