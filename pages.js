'use strict';

const fp             = require('fastify-plugin');
const { v4: uuidv4 } = require('uuid');

const { LandingPage }        = require('../models');
const { getUserLimits }      = require('../utils');
const { adminSettingsCache } = require('../adminSettings');
const {
  getUserCache, invalidateUserCache,
  getPublicCache, setPublicCache, invalidatePublicCache
} = require('../cache');

// ==================== SCHEMAS ====================
const saveSchema = {
  body: {
    type: 'object',
    required: ['title', 'config'],
    properties: {
      shortId: { type: 'string', maxLength: 50 },
      title:   { type: 'string', minLength: 1, maxLength: 200 },
      config:  {
        type: 'object',
        required: ['blocks'],
        properties: {
          blocks: { type: 'array', minItems: 1, maxItems: 100 }
        }
      }
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

// ==================== BLOCK SANITIZER ====================
function sanitizeBlock(b) {
  if (!b || b.isEditor) return null;
  if (b.id && (String(b.id).includes('editor-') || String(b.id).includes('control-'))) return null;

  switch (b.type) {
    case 'text':
      return { type: 'text', tag: b.tag || 'p', content: String(b.content || '').trim() };
    case 'image':
      return b.src ? { type: 'image', src: String(b.src).trim().slice(0, 2000) } : null;
    case 'button':
      return b.text ? { type: 'button', text: String(b.text).trim().slice(0, 200), href: String(b.href || '').trim().slice(0, 2000) } : null;
    case 'form':
      return b.html
        ? { type: 'form', html: String(b.html).replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '') }
        : null;
    default:
      return null;
  }
}

// ==================== PLUGIN ====================
module.exports = fp(async function pagesRoutes(fastify) {
  await fastify.register(require('../middleware/auth'));

  // GET /api/pages
  fastify.get('/pages', { preHandler: fastify.authenticate }, async (req, reply) => {
    const bucket = getUserCache(req.user.id);
    const TTL    = 5 * 60 * 1000;
    const now    = Date.now();

    if (bucket.pages && now - bucket.pagesTs < TTL) {
      return reply.send({ pages: bucket.pages });
    }

    const pages = await LandingPage.find({ userId: req.user.id }).sort({ updatedAt: -1 }).lean();
    const base  = `${req.protocol}://${req.hostname}`;

    const formatted = pages.map(p => ({
      shortId:   p.shortId,
      title:     p.title,
      createdAt: p.createdAt,
      updatedAt: p.updatedAt,
      url:       `${base}/p/${p.shortId}`
    }));

    bucket.pages  = formatted;
    bucket.pagesTs = now;
    return reply.send({ pages: formatted });
  });

  // GET /api/page/:shortId  (public)
  fastify.get('/page/:shortId', async (req, reply) => {
    const { shortId } = req.params;
    const cacheKey    = `apiPage:${shortId}`;
    const cached      = getPublicCache(cacheKey);
    if (cached) return reply.send(cached);

    const page = await LandingPage.findOne({ shortId }).lean();
    if (!page) return reply.status(404).send({ error: 'Page not found' });

    const data = { shortId: page.shortId, title: page.title, config: page.config };
    setPublicCache(cacheKey, data);
    return reply.send(data);
  });

  // POST /api/pages/save
  fastify.post('/pages/save', { schema: saveSchema, preHandler: fastify.authenticate }, async (req, reply) => {
    const { shortId, title, config } = req.body;
    const limits = getUserLimits(req.user, adminSettingsCache);

    if (!shortId) {
      const count = await LandingPage.countDocuments({ userId: req.user.id });
      if (limits.maxLandingPages !== Infinity && count >= limits.maxLandingPages) {
        return reply.status(403).send({ error: `Free plan allows up to ${limits.maxLandingPages} landing pages.` });
      }
    } else {
      // Ensure user owns the page they're updating
      const existing = await LandingPage.findOne({ shortId }).lean();
      if (existing && existing.userId !== req.user.id) {
        return reply.status(403).send({ error: 'You do not own this page.' });
      }
    }

    const cleanBlocks = config.blocks.map(sanitizeBlock).filter(Boolean);
    if (!cleanBlocks.length) {
      return reply.status(400).send({ error: 'No valid blocks provided.' });
    }

    const finalId = shortId || uuidv4().slice(0, 8);
    const now     = new Date();

    await LandingPage.findOneAndUpdate(
      { shortId: finalId },
      {
        $set:      { userId: req.user.id, title: title.trim(), config: { blocks: cleanBlocks }, updatedAt: now },
        $setOnInsert: { createdAt: now }
      },
      { upsert: true }
    );

    invalidateUserCache(req.user.id, 'pages');
    invalidatePublicCache(`landing:${finalId}`);
    invalidatePublicCache(`apiPage:${finalId}`);

    return reply.send({
      success: true,
      shortId: finalId,
      url:     `${req.protocol}://${req.hostname}/p/${finalId}`
    });
  });

  // POST /api/pages/delete
  fastify.post('/pages/delete', { schema: deleteSchema, preHandler: fastify.authenticate }, async (req, reply) => {
    const { shortId } = req.body;
    const page = await LandingPage.findOne({ shortId, userId: req.user.id });
    if (!page) return reply.status(404).send({ error: 'Page not found.' });

    await LandingPage.deleteOne({ shortId });
    invalidateUserCache(req.user.id, 'pages');
    invalidatePublicCache(`landing:${shortId}`);
    invalidatePublicCache(`apiPage:${shortId}`);

    return reply.send({ success: true });
  });
});
