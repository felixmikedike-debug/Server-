'use strict';

const fp = require('fastify-plugin');

const { LandingPage, FormPage }                    = require('../models');
const { textToHtmlForDisplay }                     = require('../utils');
const { getPublicCache, setPublicCache }            = require('../cache');

module.exports = fp(async function publicRoutes(fastify) {

  // GET /p/:shortId — Render landing page
  fastify.get('/p/:shortId', async (req, reply) => {
    const { shortId } = req.params;
    const cacheKey    = `landing:${shortId}`;
    const cached      = getPublicCache(cacheKey);

    if (cached) return reply.view('landing', cached);

    const page = await LandingPage.findOne({ shortId }).lean();
    if (!page) return reply.status(404).view('404');

    const processedBlocks = (page.config?.blocks || []).map(block =>
      block.type === 'text'
        ? { ...block, htmlContent: textToHtmlForDisplay(block.content) }
        : block
    );

    const data = { title: page.title, blocks: processedBlocks };
    setPublicCache(cacheKey, data);
    return reply.view('landing', data);
  });

  // GET /f/:shortId — Render form page
  fastify.get('/f/:shortId', async (req, reply) => {
    const { shortId } = req.params;
    const cacheKey    = `form:${shortId}`;
    const cached      = getPublicCache(cacheKey);

    if (cached) return reply.view('form', cached);

    const form = await FormPage.findOne({ shortId }).lean();
    if (!form) return reply.status(404).view('404');

    const data = { title: form.title, state: form.state };
    setPublicCache(cacheKey, data);
    return reply.view('form', data);
  });
});
