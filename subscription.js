'use strict';

const fp     = require('fastify-plugin');
const axios  = require('axios');
const crypto = require('crypto');

const { PAYSTACK_SECRET_KEY, MONTHLY_PRICE_KOBO } = require('../config');
const { User }                  = require('../models');
const { hasActiveSubscription } = require('../utils');

const SUCCESS_PAGE_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Payment Successful</title>
  <style>
    *{box-sizing:border-box}
    body{font-family:system-ui,sans-serif;background:#0a0a0a;color:#00ff41;display:flex;
         justify-content:center;align-items:center;min-height:100vh;margin:0;}
    .box{background:#111;padding:60px;border-radius:20px;text-align:center;
         box-shadow:0 0 30px rgba(0,255,65,.2);max-width:500px;width:90%;}
    h1{margin:0 0 20px;font-size:2.5em;color:#00ff41}
    p{font-size:1.2em;margin:16px 0;line-height:1.6}
    a{display:inline-block;margin-top:24px;padding:14px 32px;background:#00ff41;
      color:#000;font-weight:bold;text-decoration:none;border-radius:8px;font-size:1em;}
    a:hover{background:#00cc33}
  </style>
</head>
<body>
  <div class="box">
    <h1>✓ Payment Successful!</h1>
    <p>Your subscription is now <strong>active</strong>.</p>
    <p>Unlimited broadcasts, landing pages, and forms.</p>
    <a href="/">← Back to Dashboard</a>
  </div>
</body>
</html>`;

module.exports = fp(async function subscriptionRoutes(fastify) {
  await fastify.register(require('../middleware/auth'));

  // GET /api/subscription/status
  fastify.get('/status', { preHandler: fastify.authenticate }, async (req, reply) => {
    const subscribed = hasActiveSubscription(req.user);
    return reply.send({
      subscribed,
      plan:     subscribed ? 'premium-monthly' : 'free',
      endDate:  req.user.subscriptionEndDate || null,
      daysLeft: subscribed
        ? Math.ceil((new Date(req.user.subscriptionEndDate) - new Date()) / 86_400_000)
        : 0
    });
  });

  // POST /api/subscription/initiate
  fastify.post('/initiate', { preHandler: fastify.authenticate }, async (req, reply) => {
    if (hasActiveSubscription(req.user)) {
      return reply.status(400).send({ error: 'You already have an active subscription.' });
    }
    if (!PAYSTACK_SECRET_KEY) {
      return reply.status(503).send({ error: 'Payment service is not configured.' });
    }

    let data;
    try {
      const res = await axios.post(
        'https://api.paystack.co/transaction/initialize',
        {
          email:        req.user.email,
          amount:       MONTHLY_PRICE_KOBO,
          currency:     'NGN',
          callback_url: `${req.protocol}://${req.hostname}/subscription-success`,
          metadata:     { userId: req.user.id, plan: 'premium-monthly' }
        },
        {
          headers: {
            Authorization:  `Bearer ${PAYSTACK_SECRET_KEY}`,
            'Content-Type': 'application/json'
          },
          timeout: 15000
        }
      );
      data = res.data.data;
    } catch (err) {
      req.log.error({ err }, 'Paystack init error');
      return reply.status(502).send({ error: 'Failed to initialize payment. Please try again.' });
    }

    req.user.pendingPaymentReference = data.reference;
    await req.user.save();

    return reply.send({
      success:          true,
      authorizationUrl: data.authorization_url,
      reference:        data.reference
    });
  });

  // POST /api/subscription/webhook  (called by Paystack, no auth)
  fastify.post('/webhook', {
    config: { rawBody: true }  // required for HMAC verification
  }, async (req, reply) => {
    const signature = req.headers['x-paystack-signature'];
    if (!signature) return reply.status(401).send('Missing signature');

    // Use rawBody if available, fall back to JSON stringify
    const rawBody = req.rawBody || JSON.stringify(req.body);
    const hash = crypto.createHmac('sha512', PAYSTACK_SECRET_KEY)
      .update(rawBody)
      .digest('hex');

    if (hash !== signature) {
      req.log.warn('Invalid Paystack webhook signature');
      return reply.status(401).send('Invalid signature');
    }

    const event = req.body;

    if (event.event === 'charge.success') {
      const reference = event.data?.reference;
      const userId    = event.data?.metadata?.userId;

      if (userId && reference) {
        const user = await User.findOne({ id: userId });
        if (user && user.pendingPaymentReference === reference) {
          const endDate = new Date();
          endDate.setDate(endDate.getDate() + 30);

          user.isSubscribed             = true;
          user.subscriptionEndDate      = endDate;
          user.subscriptionPlan         = 'premium-monthly';
          user.pendingPaymentReference  = undefined;
          await user.save();

          req.log.info(`Subscription activated for ${user.email} (ref: ${reference})`);
        }
      }
    }

    return reply.status(200).send('OK');
  });

  // GET /subscription-success (redirect landing after payment)
  fastify.get('/subscription-success', { logLevel: 'silent' }, async (req, reply) => {
    return reply.type('text/html').send(SUCCESS_PAGE_HTML);
  });
});
