'use strict';

require('dotenv').config();

// ==================== IMPORTS ====================
const Fastify        = require('fastify');
const bcrypt         = require('bcryptjs');
const jwt            = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const { Telegraf }   = require('telegraf');
const path           = require('path');
const mongoose       = require('mongoose');
const axios          = require('axios');
const crypto         = require('crypto');
const IORedis        = require('ioredis');
const { Queue, Worker } = require('bullmq');

// ==================== FASTIFY INSTANCE ====================
const app = Fastify({
  trustProxy: true,
  logger: {
    level: process.env.LOG_LEVEL || 'info',
    transport: process.env.NODE_ENV !== 'production'
      ? { target: 'pino-pretty', options: { colorize: true } }
      : undefined,
  },
});

// ==================== PLUGINS ====================
app.register(require('@fastify/cors'),    { origin: true });
app.register(require('@fastify/static'),  { root: path.join(__dirname, 'public'), prefix: '/' });
app.register(require('@fastify/view'),    { engine: { ejs: require('ejs') }, root: path.join(__dirname, 'views') });
app.register(require('@fastify/formbody'));
app.register(require('@fastify/rate-limit'), {
  global: false,
  redis:  null,
});

// ==================== CONFIG & SECRETS ====================
const PORT               = parseInt(process.env.PORT || '3000', 10);
const JWT_SECRET         = process.env.JWT_SECRET         || 'fallback_weak_secret_change_me_immediately';
const PAYSTACK_SECRET_KEY= process.env.PAYSTACK_SECRET_KEY|| 'sk_test_fallback_change_me';
const ADMIN_PASSWORD     = process.env.ADMIN_PASSWORD     || 'midas';
const DOMAIN             = process.env.DOMAIN;

if (!DOMAIN) {
  console.error('FATAL: DOMAIN environment variable is required.');
  process.exit(1);
}

let WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;
if (!WEBHOOK_SECRET || !WEBHOOK_SECRET.trim()) {
  WEBHOOK_SECRET = crypto.randomBytes(32).toString('hex');
  console.warn('⚠️  WEBHOOK_SECRET not set — generated temporary one:', WEBHOOK_SECRET);
  console.warn('   Add WEBHOOK_SECRET=' + WEBHOOK_SECRET + ' to your .env file.\n');
} else {
  console.log('✅ Webhook secret loaded.');
}

if (JWT_SECRET.includes('fallback'))        console.warn('⚠️  JWT_SECRET is insecure fallback.');
if (PAYSTACK_SECRET_KEY.startsWith('sk_test_fallback')) console.warn('⚠️  PAYSTACK_SECRET_KEY not set.');

const MONTHLY_PRICE_KOBO = 150000;
const BATCH_SIZE         = 25;
const BATCH_INTERVAL_MS  = 8000;
const MAX_MSG_LENGTH     = 4000;

// ==================== REDIS + BULLMQ ====================
const redisConnection = process.env.REDIS_URL
  ? new IORedis(process.env.REDIS_URL,   { maxRetriesPerRequest: null, enableReadyCheck: false })
  : new IORedis({ host: 'localhost', port: 6379, maxRetriesPerRequest: null, enableReadyCheck: false });

if (!process.env.REDIS_URL) console.warn('⚠️  REDIS_URL not set — using localhost:6379');

const broadcastQueue = new Queue('telegram-broadcasts', { connection: redisConnection });

// ==================== MONGODB ====================
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/sendm';
console.log('Connecting to MongoDB:', MONGODB_URI.replace(/:([^:@]+)@/, ':****@'));

mongoose.connect(MONGODB_URI, {
  serverSelectionTimeoutMS: 30000,
  socketTimeoutMS:          45000,
  connectTimeoutMS:         30000,
}).then(() => app.log.info('✅ MongoDB connected'))
  .catch(err => { console.error('MongoDB failed:', err.message); process.exit(1); });

// ==================== SCHEMAS & MODELS ====================
const userSchema = new mongoose.Schema({
  id:                     { type: String, required: true, unique: true },
  fullName:               String,
  email:                  { type: String, required: true, unique: true, lowercase: true },
  password:               String,
  telegramBotToken:       String,
  telegramChatId:         String,
  isTelegramConnected:    { type: Boolean, default: false },
  botUsername:            String,
  isSubscribed:           { type: Boolean, default: false },
  subscriptionEndDate:    Date,
  subscriptionPlan:       String,
  pendingPaymentReference:String,
}, { timestamps: true });
userSchema.index({ telegramBotToken: 1 });

const landingPageSchema = new mongoose.Schema({
  shortId: { type: String, required: true, unique: true },
  userId:  { type: String, required: true },
  title:   String,
  config:  Object,
}, { timestamps: true });
landingPageSchema.index({ userId: 1 });

const formPageSchema = new mongoose.Schema({
  shortId:        { type: String, required: true, unique: true },
  userId:         { type: String, required: true },
  title:          String,
  state:          Object,
  welcomeMessage: String,
}, { timestamps: true });
formPageSchema.index({ userId: 1 });

const contactSchema = new mongoose.Schema({
  userId:         { type: String, required: true },
  shortId:        String,
  name:           String,
  contact:        { type: String, required: true },
  telegramChatId: String,
  status:         { type: String, default: 'pending' },
  submittedAt:    Date,
  subscribedAt:   Date,
  unsubscribedAt: Date,
}, { timestamps: true });
contactSchema.index({ userId: 1 });
contactSchema.index({ userId: 1, contact: 1 });
contactSchema.index({ userId: 1, telegramChatId: 1 });
contactSchema.index({ userId: 1, status: 1 });

const scheduledBroadcastSchema = new mongoose.Schema({
  broadcastId:  { type: String, required: true, unique: true },
  userId:       { type: String, required: true },
  message:      String,
  recipients:   { type: String, default: 'all' },
  scheduledTime:Date,
  status:       { type: String, default: 'pending' },
}, { timestamps: true });
scheduledBroadcastSchema.index({ userId: 1 });
scheduledBroadcastSchema.index({ status: 1 });
scheduledBroadcastSchema.index({ scheduledTime: 1 });

const broadcastDailySchema = new mongoose.Schema({
  userId: { type: String, required: true },
  date:   { type: String, required: true },
  count:  { type: Number, default: 1 },
}, { timestamps: true });
broadcastDailySchema.index({ userId: 1, date: 1 }, { unique: true });

const adminSettingsSchema = new mongoose.Schema({
  dailyBroadcastLimit: { type: Number, default: 3, min: 1 },
  maxLandingPages:     { type: Number, default: 5, min: 1 },
  maxForms:            { type: Number, default: 5, min: 1 },
}, { timestamps: true });

adminSettingsSchema.statics.getSettings = async function () {
  return (await this.findOne()) || await this.create({ dailyBroadcastLimit: 3, maxLandingPages: 5, maxForms: 5 });
};
adminSettingsSchema.statics.updateSettings = async function (updates) {
  const s = (await this.findOne()) || new this();
  Object.assign(s, updates);
  await s.save();
  return s;
};

const User              = mongoose.model('User',              userSchema);
const LandingPage       = mongoose.model('LandingPage',       landingPageSchema);
const FormPage          = mongoose.model('FormPage',          formPageSchema);
const Contact           = mongoose.model('Contact',           contactSchema);
const ScheduledBroadcast= mongoose.model('ScheduledBroadcast',scheduledBroadcastSchema);
const BroadcastDaily    = mongoose.model('BroadcastDaily',    broadcastDailySchema);
const AdminSettings     = mongoose.model('AdminSettings',     adminSettingsSchema);

// ==================== IN-MEMORY STATE ====================
const activeBots          = new Map();
const resetTokens         = new Map();
const pendingSubscribers  = new Map();
const lastWebhookSetTime  = new Map();

// ==================== ADMIN SETTINGS CACHE ====================
let adminSettingsCache = { dailyBroadcastLimit: 3, maxLandingPages: 5, maxForms: 5 };

// ==================== PER-USER + PUBLIC CACHE ====================
const userCache  = new Map();
const publicCache= new Map();

const TTL = { pages: 5*60*1000, forms: 5*60*1000, contacts: 2*60*1000, public: 10*60*1000 };

function getUserCache(userId) {
  let b = userCache.get(userId);
  if (!b) {
    b = { pages: null, forms: null, contacts: null, pagesTs: 0, formsTs: 0, contactsTs: 0, lastAccess: Date.now() };
    userCache.set(userId, b);
  } else {
    b.lastAccess = Date.now();
  }
  return b;
}

function invalidateUserCache(userId, type = 'all') {
  const b = userCache.get(userId);
  if (!b) return;
  if (type === 'pages'    || type === 'all') { b.pages    = null; b.pagesTs    = 0; }
  if (type === 'forms'    || type === 'all') { b.forms    = null; b.formsTs    = 0; }
  if (type === 'contacts' || type === 'all') { b.contacts = null; b.contactsTs = 0; }
  b.lastAccess = Date.now();
}

function invalidatePublicCache(key) { publicCache.delete(key); }

// Cache GC
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of publicCache) if (now - v.timestamp > TTL.public)      publicCache.delete(k);
  for (const [k, v] of userCache)   if (now - v.lastAccess > 30*60*1000)     { userCache.delete(k); }
}, 10*60*1000);

// PendingSubscriber GC
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of pendingSubscribers) if (now - v.createdAt > 30*60*1000) pendingSubscribers.delete(k);
}, 60*60*1000);

// ==================== UTILITIES ====================
const CONTACT_REGEX = /^(\+?[0-9\s\-()]{7,20}|[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})$/;

function escapeHtml(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#039;');
}

function sanitizeTelegramHtml(unsafe) {
  if (!unsafe || typeof unsafe !== 'string') return '';
  const allowed = new Set(['b','strong','i','em','u','ins','s','strike','del','span','tg-spoiler','a','code','pre','tg-emoji','blockquote']);
  const allowedAttrs = { a: ['href'], 'tg-emoji': ['emoji-id'], blockquote: ['expandable'] };
  let clean = unsafe
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
    .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, '')
    .replace(/on\w+="[^"]*"/gi, '')
    .replace(/javascript:/gi, '');
  clean = clean.replace(/<\/?([a-z][a-z0-9]*)\b[^>]*>/gi, (match, tag) => {
    const t = tag.toLowerCase();
    if (!allowed.has(t)) return '';
    if (match.startsWith('</')) return `</${t}>`;
    let attrs = '';
    const ar = /([a-z0-9-]+)="([^"]*)"/gi;
    let m;
    while ((m = ar.exec(match)) !== null) {
      const an = m[1].toLowerCase(), av = m[2];
      if (allowedAttrs[t]?.includes(an)) {
        const safe = (an === 'href' && !/^https?:\/\//i.test(av) && !av.startsWith('/')) ? '#' : av;
        attrs += ` \( {an}=" \){safe.replace(/"/g,'&quot;')}"`;
      }
    }
    return `<\( {t} \){attrs}>`;
  });
  return clean.trim();
}

function prepareTelegramMessage(raw) {
  if (!raw || typeof raw !== 'string') return '';
  let msg = raw.trim()
    .replace(/<br\s*\/?>/gi,         '\n')
    .replace(/<\/p>\s*<p[^>]*>/gi,   '\n\n')
    .replace(/<p[^>]*>/gi,           '')
    .replace(/<\/p>/gi,              '\n')
    .replace(/<div[^>]*>/gi,         '\n')
    .replace(/<\/div>/gi,            '\n')
    .replace(/\n{3,}/g,              '\n\n');
  return sanitizeTelegramHtml(msg);
}

function splitTelegramMessage(text) {
  if (!text) return [];
  const chunks = [];
  let cur = '';
  for (const line of text.split(/\r?\n/)) {
    let l = line;
    while (l.length > MAX_MSG_LENGTH) {
      if (cur) { chunks.push(cur.trim()); cur = ''; }
      chunks.push(l.slice(0, MAX_MSG_LENGTH).trim());
      l = l.slice(MAX_MSG_LENGTH);
    }
    if (cur.length + l.length + (cur ? 1 : 0) <= MAX_MSG_LENGTH) {
      cur += (cur ? '\n' : '') + l;
    } else {
      if (cur) chunks.push(cur.trim());
      cur = l;
    }
  }
  if (cur) chunks.push(cur.trim());
  if (chunks.length <= 1) return chunks;
  const total = chunks.length;
  return chunks.map((c, i) => {
    const hdr = `(\( {i+1}/ \){total})\n\n`;
    return hdr.length + c.length > MAX_MSG_LENGTH ? c : hdr + c;
  });
}

function textToHtmlForDisplay(text) {
  if (!text) return '';
  return text.replace(/\n{2,}/g, '</p><p>').replace(/\n/g, '<br>');
}

function getTodayDateString() { return new Date().toISOString().slice(0, 10); }

function hasActiveSubscription(user) {
  return user.isSubscribed && user.subscriptionEndDate && new Date(user.subscriptionEndDate) > new Date();
}

function getUserLimits(user) {
  if (hasActiveSubscription(user))
    return { dailyBroadcasts: Infinity, maxLandingPages: Infinity, maxForms: Infinity };
  return {
    dailyBroadcasts:  adminSettingsCache.dailyBroadcastLimit,
    maxLandingPages:  adminSettingsCache.maxLandingPages,
    maxForms:         adminSettingsCache.maxForms,
  };
}

async function incrementDailyBroadcast(userId) {
  const rec = await BroadcastDaily.findOneAndUpdate(
    { userId, date: getTodayDateString() },
    { $inc: { count: 1 } },
    { upsert: true, new: true }
  );
  return rec.count;
}

function generate2FACode() { return Math.floor(100000 + Math.random() * 900000).toString(); }

async function send2FACodeViaBot(user, code) {
  if (!user.isTelegramConnected || !user.telegramChatId || !activeBots.has(user.id)) return false;
  try {
    await activeBots.get(user.id).telegram.sendMessage(
      user.telegramChatId,
      `Security Alert – Password Reset\n\nYour 6-digit code:\n\n<b>${code}</b>\n\nValid for 10 minutes.`,
      { parse_mode: 'HTML' }
    );
    return true;
  } catch (err) { console.error('2FA send failed:', err.message); return false; }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ==================== TELEGRAM BOT MANAGEMENT ====================
function launchUserBot(user) {
  if (activeBots.has(user.id)) activeBots.delete(user.id);
  if (!user.telegramBotToken) return;

  const bot = new Telegraf(user.telegramBotToken);
  bot.webhookReply = false;
  bot.options.webhookReply = false;

  bot.catch((err) => {
    if (!err.message?.includes('Bot is not running'))
      console.error(`Bot error for ${user.email}:`, err);
  });

  // /start handler
  bot.start(async (ctx) => {
    const payload = ctx.startPayload || '';
    const chatId  = ctx.chat.id.toString();

    if (payload.startsWith('sub_') && pendingSubscribers.has(payload)) {
      const sub = pendingSubscribers.get(payload);
      if (sub.userId === user.id) {
        let target = await Contact.findOne({ userId: user.id, telegramChatId: chatId });
        const byContact = await Contact.find({ userId: user.id, contact: sub.contact });

        if (!target) {
          target = byContact.find(c => c.status === 'subscribed')
                || byContact.find(c => c.shortId === sub.shortId)
                || byContact[0];
        }

        if (!target) {
          target = new Contact({
            userId: user.id, shortId: sub.shortId, name: sub.name, contact: sub.contact,
            telegramChatId: chatId, status: 'subscribed',
            submittedAt: new Date(), subscribedAt: new Date(),
          });
        } else {
          Object.assign(target, {
            name: sub.name, contact: sub.contact, shortId: sub.shortId,
            telegramChatId: chatId, status: 'subscribed',
            subscribedAt: target.subscribedAt || new Date(), submittedAt: new Date(),
          });
        }
        await target.save();

        await Contact.deleteMany({
          userId: user.id,
          $or: [
            { contact: sub.contact, _id: { $ne: target._id } },
            { telegramChatId: chatId, _id: { $ne: target._id } },
          ],
        });

        pendingSubscribers.delete(payload);
        invalidateUserCache(user.id, 'contacts');

        const form = await FormPage.findOne({ shortId: sub.shortId });
        let welcome = `<b>Subscription Confirmed!</b>\n\nHi <b>${escapeHtml(sub.name)}</b>!\n\nYou're now subscribed.\n\nThank you`;
        if (form?.welcomeMessage?.trim()) {
          welcome = form.welcomeMessage
            .replace(/\{name\}/gi,    `<b>${escapeHtml(sub.name)}</b>`)
            .replace(/\{contact\}/gi, escapeHtml(sub.contact));
        }
        await ctx.replyWithHTML(welcome);
        return;
      }
    }

    if (payload === user.id) {
      const freshUser = await User.findOne({ id: user.id });
      if (freshUser) {
        freshUser.telegramChatId      = chatId;
        freshUser.isTelegramConnected = true;
        await freshUser.save();
        user.telegramChatId      = chatId;
        user.isTelegramConnected = true;
      }
      await ctx.replyWithHTML('<b>Sendm 2FA Connected Successfully!</b>\n\nYou will receive login codes here.');
      return;
    }

    await ctx.replyWithHTML('<b>Welcome!</b>\n\nSubscribe from the page to get updates.');
  });

  bot.command('status', async (ctx) => {
    await ctx.replyWithHTML(
      `<b>Sendm 2FA Status</b>\nAccount: <code>\( {user.email}</code>\nStatus: <b> \){user.isTelegramConnected ? 'Connected' : 'Not Connected'}</b>`
    );
  });

  activeBots.set(user.id, bot);

  const webhookPath = `/webhook/\( {WEBHOOK_SECRET}/ \){user.id}`;
  const webhookUrl  = `https://\( {DOMAIN} \){webhookPath}`;

  (async () => {
    try {
      console.log(`[Webhook Setup] Starting for \( {user.email} (@ \){user.botUsername || 'unknown'})`);

      const current = await bot.telegram.getWebhookInfo().catch(() => ({}));
      const alreadyOk = current.url === webhookUrl && !current.has_custom_certificate && current.pending_update_count < 50;
      const recentlySet = (Date.now() - (lastWebhookSetTime.get(user.id) || 0)) < 30*60*1000;

      if (alreadyOk && recentlySet) {
        console.log(`[Webhook Setup] Already OK for ${user.email}`);
        return;
      }

      await bot.telegram.deleteWebhook({ drop_pending_updates: true }).catch(() => {});
      await sleep(2000);

      for (let attempt = 0; attempt < 6; attempt++) {
        try {
          const ok = await bot.telegram.setWebhook(webhookUrl, {
            allowed_updates: ['message', 'callback_query', 'my_chat_member'],
          });
          if (ok) {
            console.log(`✅ Webhook successfully set for ${user.email} → ${webhookUrl}`);
            lastWebhookSetTime.set(user.id, Date.now());

            if (user.telegramChatId) {
              await bot.telegram.sendMessage(user.telegramChatId,
                '✅ <b>Bot connected successfully!</b>\nDeep links and subscriptions should now work.',
                { parse_mode: 'HTML' }
              ).catch(() => {});
            }
            return;
          }
        } catch (err) {
          if (err.response?.error_code === 429) {
            const wait = (err.response.parameters?.retry_after || 30) + 5;
            console.warn(`[Webhook Setup] Rate-limited for ${user.email}, waiting ${wait}s`);
            await sleep(wait * 1000);
          } else {
            console.error(`[Webhook Setup] Failed for ${user.email}: ${err.message}`);
            if (attempt >= 5) throw err;
            await sleep(4000);
          }
        }
      }
    } catch (err) {
      console.error(`❌ Webhook setup failed for ${user.email}: ${err.message}`);
    }
  })();
}

// ==================== BULLMQ WORKER ====================
async function processBroadcast(job) {
  const { userId, message, broadcastId } = job.data;

  const bot = activeBots.get(userId);
  if (!bot) throw new Error('Telegram bot not connected for user ' + userId);

  const chunks  = splitTelegramMessage(message);
  const targets = await Contact.find({
    userId, status: 'subscribed', telegramChatId: { $exists: true, $ne: null },
  }).lean();

  let sent = 0, failed = 0;
  const total = targets.length;

  for (let b = 0; b < targets.length; b += BATCH_SIZE) {
    const batch = targets.slice(b, b + BATCH_SIZE);
    await Promise.all(batch.map(async (target) => {
      try {
        for (const chunk of chunks)
          await bot.telegram.sendMessage(target.telegramChatId, chunk, { parse_mode: 'HTML' });
        sent++;
      } catch (err) {
        failed++;
        const blocked = err.response?.error_code === 403 || /blocked|forbidden|chat not found|deactivated/i.test(err.message || '');
        if (blocked) await Contact.findByIdAndUpdate(target._id, { status: 'unsubscribed', unsubscribedAt: new Date(), telegramChatId: null });
      }
    }));
    if (b + BATCH_SIZE < targets.length) await sleep(BATCH_INTERVAL_MS);
  }

  const user = await User.findOne({ id: userId });
  const emoji = failed === 0 ? '✅' : '⚠️';
  let report = broadcastId ? '<b>Scheduled Broadcast Report</b>\n\n' : '<b>Broadcast Report</b>\n\n';
  report += total === 0
    ? 'No subscribed contacts with Telegram connected.'
    : `\( {emoji} <b> \){sent} of \( {total}</b> delivered. \){failed > 0 ? `\n${failed} failed.` : ''}`;
  report += `\n\nTime: ${new Date().toLocaleString()}`;

  if (user?.isTelegramConnected && user.telegramChatId && activeBots.has(userId)) {
    try { await bot.telegram.sendMessage(user.telegramChatId, report, { parse_mode: 'HTML' }); }
    catch (e) { console.error('Failed to send report:', e.message); }
  }

  if (broadcastId) await ScheduledBroadcast.deleteOne({ broadcastId });
  invalidateUserCache(userId, 'contacts');
}

const worker = new Worker('telegram-broadcasts', processBroadcast, { connection: redisConnection, concurrency: 4 });

worker.on('completed', (job) => app.log.info(`Broadcast job ${job.id} completed for user ${job.data.userId}`));

worker.on('failed', async (job, err) => {
  app.log.error(`Broadcast job ${job?.id} failed: ${err.message}`);
  const { userId, broadcastId } = job?.data || {};
  if (broadcastId) await ScheduledBroadcast.findOneAndUpdate({ broadcastId }, { status: 'failed' }).catch(() => {});
  const user = await User.findOne({ id: userId });
  if (user?.isTelegramConnected && user.telegramChatId && activeBots.has(userId)) {
    const bot  = activeBots.get(userId);
    const text = `<b>${broadcastId ? 'Scheduled ' : ''}Broadcast Failed</b>\n\nError: ${err.message}`;
    try { await bot.telegram.sendMessage(user.telegramChatId, text, { parse_mode: 'HTML' }); } catch {}
  }
});

// ==================== SCHEDULED BROADCAST RECOVERY ====================
async function recoverScheduledBroadcasts() {
  const pending = await ScheduledBroadcast.find({ status: 'pending', scheduledTime: { $gt: new Date() } }).lean();
  if (!pending.length) { console.log('✓ No broadcasts need recovery.'); return; }

  let recovered = 0, existed = 0;
  for (const task of pending) {
    const existing = await broadcastQueue.getJob(task.broadcastId);
    if (existing) { existed++; continue; }
    const delayMs = Math.max(0, task.scheduledTime.getTime() - Date.now());
    await broadcastQueue.add('send-broadcast',
      { userId: task.userId, message: task.message, broadcastId: task.broadcastId },
      { jobId: task.broadcastId, delay: delayMs, attempts: 4, backoff: { type: 'exponential', delay: 5000 } }
    );
    recovered++;
  }
  console.log(`✓ Recovery: ${recovered} re-queued, ${existed} already present.`);
}

// ==================== JWT AUTH DECORATOR ====================
async function authenticate(req, reply) {
  const header = req.headers.authorization;
  const token  = (header?.startsWith('Bearer ') ? header.split(' ')[1] : null) || req.query.token;
  if (!token) return reply.code(401).send({ error: 'Access token required' });

  let decoded;
  try { decoded = jwt.verify(token, JWT_SECRET); }
  catch { return reply.code(403).send({ error: 'Invalid or expired token' }); }

  const user = await User.findOne({ id: decoded.userId });
  if (!user) return reply.code(404).send({ error: 'User not found' });
  req.user = user;
}

app.decorate('authenticate', authenticate);

// ==================== RATE LIMITERS ====================
const authRateLimit = {
  config: { rateLimit: { max: 10, timeWindow: '15 minutes' } },
};
const formSubmitRateLimit = {
  config: { rateLimit: { max: 10, timeWindow: '15 minutes', keyGenerator: (req) => `\( {req.ip}:: \){req.params.shortId}` } },
};

// ==================== WEBHOOK ENDPOINT ====================
app.post(`/webhook/${WEBHOOK_SECRET}/:userId`, {
  config: { rawBody: true },
}, async (req, reply) => {
  const userId = req.params.userId;
  const bot    = activeBots.get(userId);

  if (!bot) {
    console.warn(`Webhook received for unknown bot: ${userId}`);
    return reply.code(200).send('OK');
  }

  let update;
  try {
    update = typeof req.body === 'object' ? req.body : JSON.parse(req.body);
  } catch {
    return reply.code(400).send('Bad Request');
  }

  try { await bot.handleUpdate(update); }
  catch (err) { app.log.error(`Webhook handle error for ${userId}: ${err.message}`); }

  reply.code(200).send('OK');
});

// ==================== AUTH ROUTES ====================
app.post('/api/auth/register', authRateLimit, async (req, reply) => {
  const { fullName, email, password } = req.body || {};
  if (!fullName?.trim() || !email?.trim() || !password?.trim())
    return reply.code(400).send({ error: 'All fields required' });
  if (password.length < 6)
    return reply.code(400).send({ error: 'Password must be at least 6 characters' });

  const exists = await User.findOne({ email: email.toLowerCase() });
  if (exists) return reply.code(409).send({ error: 'Email already registered' });

  const hashed = await bcrypt.hash(password, 12);
  const user   = await User.create({ id: uuidv4(), fullName: fullName.trim(), email: email.toLowerCase(), password: hashed });
  const token  = jwt.sign({ userId: user.id }, JWT_SECRET, { expiresIn: '7d' });

  reply.code(201).send({ success: true, token, user: { id: user.id, fullName: user.fullName, email: user.email, isTelegramConnected: false } });
});

app.post('/api/auth/login', authRateLimit, async (req, reply) => {
  const { email, password } = req.body || {};
  if (!email?.trim() || !password?.trim())
    return reply.code(400).send({ error: 'Email and password required' });

  const user = await User.findOne({ email: email.toLowerCase() });
  if (!user || !(await bcrypt.compare(password, user.password)))
    return reply.code(401).send({ error: 'Invalid credentials' });

  const token = jwt.sign({ userId: user.id }, JWT_SECRET, { expiresIn: '7d' });
  reply.send({ success: true, token, user: { id: user.id, fullName: user.fullName, email: user.email, isTelegramConnected: user.isTelegramConnected } });
});

app.get('/api/auth/me', { preHandler: app.authenticate }, async (req, reply) => {
  reply.send({ user: { id: req.user.id, fullName: req.user.fullName, email: req.user.email, isTelegramConnected: req.user.isTelegramConnected } });
});

async function validateBotToken(token, maxAttempts = 5) {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      const res = await axios.get(`https://api.telegram.org/bot${token}/getMe`, { timeout: 20000 });
      if (!res.data.ok) throw new Error(res.data.description || 'Unauthorized');
      if (!res.data.result?.username) throw new Error('Missing bot username in response');
      return res.data.result;
    } catch (err) {
      if (i >= maxAttempts - 1) throw err;
      if (err.response?.status === 401) throw err;
      await sleep(8000);
    }
  }
}

app.post('/api/auth/connect-telegram', { preHandler: app.authenticate }, async (req, reply) => {
  const token = req.body?.botToken?.trim();
  if (!token) return reply.code(400).send({ error: 'Bot token required' });

  const existing = await User.findOne({ telegramBotToken: token });
  if (existing && existing.id !== req.user.id)
    return reply.code(400).send({ error: 'This bot is already linked to another account.' });

  let botInfo;
  try { botInfo = await validateBotToken(token); }
  catch (err) {
    const msg = err.response?.data?.description || err.message;
    if (/unauthorized|not found/i.test(msg)) return reply.code(400).send({ error: 'Invalid bot token: ' + msg });
    return reply.code(500).send({ error: 'Network error validating bot token. Please try again.' });
  }

  const botUsername = botInfo.username.replace(/^@/, '');

  if (req.user.telegramBotToken && req.user.telegramBotToken !== token) {
    await axios.post(`https://api.telegram.org/bot${req.user.telegramBotToken}/deleteWebhook`, { drop_pending_updates: true }, { timeout: 20000 }).catch(() => {});
  }

  Object.assign(req.user, { telegramBotToken: token, botUsername, isTelegramConnected: false, telegramChatId: null });
  await req.user.save();

  launchUserBot(req.user.toObject ? req.user.toObject() : { ...req.user });

  await sleep(2500);

  reply.send({ success: true, message: 'Bot connected!', botUsername: '@' + botUsername, startLink: `https://t.me/\( {botUsername}?start= \){req.user.id}` });
});

app.post('/api/auth/change-bot-token', { preHandler: app.authenticate }, async (req, reply) => {
  const token = req.body?.newBotToken?.trim();
  if (!token) return reply.code(400).send({ error: 'New bot token required' });

  const existing = await User.findOne({ telegramBotToken: token });
  if (existing && existing.id !== req.user.id)
    return reply.code(400).send({ error: 'This bot is already linked to another account.' });

  let botInfo;
  try { botInfo = await validateBotToken(token); }
  catch (err) {
    const msg = err.response?.data?.description || err.message;
    if (/unauthorized|not found/i.test(msg)) return reply.code(400).send({ error: 'Invalid bot token: ' + msg });
    return reply.code(500).send({ error: 'Network error validating token. Please try again.' });
  }

  const botUsername = botInfo.username.replace(/^@/, '');
  if (req.user.telegramBotToken)
    await axios.post(`https://api.telegram.org/bot${req.user.telegramBotToken}/deleteWebhook`, { drop_pending_updates: true }, { timeout: 20000 }).catch(() => {});

  Object.assign(req.user, { telegramBotToken: token, botUsername, isTelegramConnected: false, telegramChatId: null });
  await req.user.save();

  launchUserBot(req.user.toObject ? req.user.toObject() : { ...req.user });

  await sleep(2500);

  reply.send({ success: true, message: 'Bot token updated! Send /start to the new bot to reconnect 2FA.', botUsername: '@' + botUsername, startLink: `https://t.me/\( {botUsername}?start= \){req.user.id}` });
});

app.post('/api/auth/disconnect-telegram', { preHandler: app.authenticate }, async (req, reply) => {
  if (req.user.telegramBotToken)
    await axios.post(`https://api.telegram.org/bot${req.user.telegramBotToken}/deleteWebhook`, { drop_pending_updates: true }, { timeout: 20000 }).catch(() => {});

  activeBots.delete(req.user.id);
  Object.assign(req.user, { telegramBotToken: null, botUsername: null, telegramChatId: null, isTelegramConnected: false });
  await req.user.save();

  reply.send({ success: true, message: 'Telegram disconnected. You can now connect a fresh bot.' });
});

app.get('/api/auth/bot-status', { preHandler: app.authenticate }, (req, reply) => {
  reply.send({ activated: req.user.isTelegramConnected, chatId: req.user.telegramChatId || null });
});

// ==================== PASSWORD RESET ====================
app.post('/api/auth/forgot-password', authRateLimit, async (req, reply) => {
  const { email } = req.body || {};
  if (!email?.trim()) return reply.code(400).send({ error: 'Email required' });

  const user = await User.findOne({ email: email.toLowerCase() });
  if (!user || !user.isTelegramConnected)
    return reply.send({ success: true, message: 'If that account has 2FA enabled, a code was sent.' });

  const code       = generate2FACode();
  const resetToken = uuidv4();
  resetTokens.set(resetToken, { userId: user.id, code, expiresAt: Date.now() + 10*60*1000 });

  const sent = await send2FACodeViaBot(user, code);
  if (!sent) return reply.code(500).send({ error: 'Failed to send code via Telegram.' });

  reply.send({ success: true, message: 'Code sent!', resetToken });
});

app.post('/api/auth/verify-reset-code', async (req, reply) => {
  const { resetToken, code } = req.body || {};
  if (!resetToken?.trim() || !code?.trim()) return reply.code(400).send({ error: 'Token and code required' });

  const entry = resetTokens.get(resetToken);
  if (!entry || Date.now() > entry.expiresAt) {
    resetTokens.delete(resetToken);
    return reply.code(400).send({ error: 'Invalid or expired code' });
  }
  if (entry.code !== code.trim()) return reply.code(400).send({ error: 'Wrong code' });

  reply.send({ success: true, message: 'Code verified' });
});

app.post('/api/auth/reset-password', async (req, reply) => {
  const { resetToken, newPassword } = req.body || {};
  if (!resetToken?.trim() || !newPassword || newPassword.length < 6)
    return reply.code(400).send({ error: 'Valid token and password (≥6 chars) required' });

  const entry = resetTokens.get(resetToken);
  if (!entry || Date.now() > entry.expiresAt) {
    resetTokens.delete(resetToken);
    return reply.code(400).send({ error: 'Invalid or expired session' });
  }

  const user = await User.findOne({ id: entry.userId });
  if (!user) return reply.code(404).send({ error: 'User not found' });

  user.password = await bcrypt.hash(newPassword, 12);
  await user.save();
  resetTokens.delete(resetToken);

  reply.send({ success: true, message: 'Password reset successful' });
});

// ==================== SUBSCRIPTION ====================
app.get('/api/subscription/status', { preHandler: app.authenticate }, async (req, reply) => {
  const subscribed = hasActiveSubscription(req.user);
  reply.send({
    subscribed,
    plan:     subscribed ? 'premium-monthly' : 'free',
    endDate:  req.user.subscriptionEndDate || null,
    daysLeft: subscribed ? Math.ceil((new Date(req.user.subscriptionEndDate) - Date.now()) / 86400000) : 0,
  });
});

app.post('/api/subscription/initiate', { preHandler: app.authenticate }, async (req, reply) => {
  if (hasActiveSubscription(req.user)) return reply.code(400).send({ error: 'You already have an active subscription' });

  try {
    const res = await axios.post(
      'https://api.paystack.co/transaction/initialize',
      {
        email:        req.user.email,
        amount:       MONTHLY_PRICE_KOBO,
        currency:     'NGN',
        callback_url: `\( {req.protocol}:// \){req.hostname}/subscription-success`,
        metadata:     { userId: req.user.id, plan: 'premium-monthly' },
      },
      { headers: { Authorization: `Bearer ${PAYSTACK_SECRET_KEY}`, 'Content-Type': 'application/json' } }
    );

    const { authorization_url, reference } = res.data.data;
    req.user.pendingPaymentReference = reference;
    await req.user.save();

    reply.send({ success: true, authorizationUrl: authorization_url, reference });
  } catch (err) {
    app.log.error('Paystack init error:', err.response?.data || err.message);
    reply.code(500).send({ error: 'Failed to initialize payment' });
  }
});

app.post('/api/subscription/webhook', {
  config: { rawBody: true },
}, async (req, reply) => {
  try {
    const rawBody = req.rawBody || JSON.stringify(req.body);
    const hash = crypto.createHmac('sha512', PAYSTACK_SECRET_KEY).update(rawBody).digest('hex');

    if (hash !== req.headers['x-paystack-signature'])
      return reply.code(401).send('Invalid signature');

    const event = req.body;
    if (event.event === 'charge.success') {
      const { reference, metadata } = event.data;
      const userId = metadata?.userId;
      if (!userId) return reply.send('OK');

      const user = await User.findOne({ id: userId });
      if (!user || user.pendingPaymentReference !== reference) return reply.send('OK');

      const endDate = new Date();
      endDate.setDate(endDate.getDate() + 30);
      Object.assign(user, { isSubscribed: true, subscriptionEndDate: endDate, subscriptionPlan: 'premium-monthly', pendingPaymentReference: undefined });
      await user.save();
      app.log.info(`Subscription activated for ${user.email} (ref: ${reference})`);
    }

    reply.send('OK');
  } catch (err) {
    app.log.error('Paystack webhook error:', err);
    reply.send('OK');
  }
});

app.get('/subscription-success', (req, reply) => {
  reply.type('text/html').send(`<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>Payment Successful</title>
<style>body{font-family:system-ui,sans-serif;background:#0a0a0a;color:#00ff41;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0}
.box{background:#111;padding:60px;border-radius:20px;text-align:center;box-shadow:0 0 30px rgba(0,255,65,.2)}
h1{margin:0 0 20px;font-size:3em;color:#00ff41}p{font-size:1.3em;margin:20px 0;line-height:1.6}
a{display:inline-block;margin-top:30px;padding:14px 32px;background:#00ff41;color:#000;font-weight:700;text-decoration:none;border-radius:8px;font-size:1.1em}
a:hover{background:#00cc33}</style></head>
<body><div class="box"><h1>✓ Payment Successful!</h1><p>Your subscription is now <strong>active</strong>.</p>
<p>You have unlimited broadcasts, landing pages, and forms.</p>
<p><a href="https://${DOMAIN}">← Return to Dashboard</a></p></div></body></html>`);
});

// ==================== PUBLIC PAGE RENDERING ====================
app.get('/p/:shortId', async (req, reply) => {
  const key    = `landing:${req.params.shortId}`;
  const cached = publicCache.get(key);
  if (cached && Date.now() - cached.timestamp < TTL.public) return reply.view('landing', cached.data);

  const page = await LandingPage.findOne({ shortId: req.params.shortId });
  if (!page) return reply.code(404).view('404');

  const processedBlocks = page.config.blocks.map(b =>
    b.type === 'text' ? { ...b, htmlContent: textToHtmlForDisplay(b.content) } : b
  );
  const data = { title: page.title, blocks: processedBlocks };
  publicCache.set(key, { data, timestamp: Date.now() });
  reply.view('landing', data);
});

app.get('/f/:shortId', async (req, reply) => {
  const key    = `form:${req.params.shortId}`;
  const cached = publicCache.get(key);
  if (cached && Date.now() - cached.timestamp < TTL.public) return reply.view('form', cached.data);

  const form = await FormPage.findOne({ shortId: req.params.shortId });
  if (!form) return reply.code(404).view('404');

  const data = { title: form.title, state: form.state };
  publicCache.set(key, { data, timestamp: Date.now() });
  reply.view('form', data);
});

// ==================== API READ ROUTES ====================
app.get('/api/pages', { preHandler: app.authenticate }, async (req, reply) => {
  const bucket = getUserCache(req.user.id);
  const now    = Date.now();
  if (bucket.pages && now - bucket.pagesTs < TTL.pages) return reply.send({ pages: bucket.pages });

  const pages   = await LandingPage.find({ userId: req.user.id }).sort({ updatedAt: -1 });
  const base    = `\( {req.protocol}:// \){req.hostname}`;
  const formatted = pages.map(p => ({ shortId: p.shortId, title: p.title, createdAt: p.createdAt, updatedAt: p.updatedAt, url: `\( {base}/p/ \){p.shortId}` }));

  bucket.pages  = formatted; bucket.pagesTs = now;
  reply.send({ pages: formatted });
});

app.get('/api/forms', { preHandler: app.authenticate }, async (req, reply) => {
  const bucket = getUserCache(req.user.id);
  const now    = Date.now();
  if (bucket.forms && now - bucket.formsTs < TTL.forms) return reply.send({ forms: bucket.forms });

  const forms   = await FormPage.find({ userId: req.user.id }).sort({ updatedAt: -1 });
  const base    = `\( {req.protocol}:// \){req.hostname}`;
  const formatted = forms.map(f => ({ shortId: f.shortId, title: f.title, createdAt: f.createdAt, updatedAt: f.updatedAt, url: `\( {base}/f/ \){f.shortId}` }));

  bucket.forms  = formatted; bucket.formsTs = now;
  reply.send({ forms: formatted });
});

app.get('/api/contacts', { preHandler: app.authenticate }, async (req, reply) => {
  const bucket = getUserCache(req.user.id);
  const now    = Date.now();
  if (bucket.contacts && now - bucket.contactsTs < TTL.contacts) return reply.send({ success: true, contacts: bucket.contacts });

  const contacts  = await Contact.find({ userId: req.user.id }).sort({ submittedAt: -1 });
  const formatted = contacts.map(c => ({
    name:          c.name,
    contact:       c.contact,
    status:        c.status,
    telegramChatId:c.telegramChatId || null,
    pageId:        c.shortId,
    submittedAt:   c.submittedAt ? new Date(c.submittedAt).toLocaleString() : null,
    subscribedAt:  c.subscribedAt ? new Date(c.subscribedAt).toLocaleString() : null,
  }));

  bucket.contacts  = formatted; bucket.contactsTs = now;
  reply.send({ success: true, contacts: formatted });
});

app.get('/api/page/:shortId', async (req, reply) => {
  const key = `apiPage:${req.params.shortId}`;
  const cached = publicCache.get(key);
  if (cached && Date.now() - cached.timestamp < TTL.public) return reply.send(cached.data);

  const page = await LandingPage.findOne({ shortId: req.params.shortId });
  if (!page) return reply.code(404).send({ error: 'Page not found' });

  const data = { shortId: page.shortId, title: page.title, config: page.config };
  publicCache.set(key, { data, timestamp: Date.now() });
  reply.send(data);
});

app.get('/api/form/:shortId', async (req, reply) => {
  const key = `apiForm:${req.params.shortId}`;
  const cached = publicCache.get(key);
  if (cached && Date.now() - cached.timestamp < TTL.public) return reply.send(cached.data);

  const form = await FormPage.findOne({ shortId: req.params.shortId });
  if (!form) return reply.code(404).send({ error: 'Form not found' });

  const data = { shortId: form.shortId, title: form.title, state: form.state, welcomeMessage: form.welcomeMessage };
  publicCache.set(key, { data, timestamp: Date.now() });
  reply.send(data);
});

// ==================== LANDING PAGES WRITE ====================
app.post('/api/pages/save', { preHandler: app.authenticate }, async (req, reply) => {
  const { shortId, title, config } = req.body || {};
  if (!title?.trim() || !config || !Array.isArray(config.blocks))
    return reply.code(400).send({ error: 'title and config.blocks (array) required' });

  const limits = getUserLimits(req.user);

  if (!shortId) {
    const count = await LandingPage.countDocuments({ userId: req.user.id });
    if (limits.maxLandingPages !== Infinity && count >= limits.maxLandingPages)
      return reply.code(403).send({ error: `Free tier limit: ${limits.maxLandingPages} landing pages.` });
  } else {
    const existing = await LandingPage.findOne({ shortId });
    if (existing && existing.userId !== req.user.id)
      return reply.code(403).send({ error: 'Not your page' });
  }

  const cleanBlocks = config.blocks.map(b => {
    if (!b || b.isEditor) return null;
    switch (b.type) {
      case 'text':   return { type: 'text',   tag: b.tag || 'p', content: (b.content || '').trim() };
      case 'image':  return b.src  ? { type: 'image',  src: b.src.trim() } : null;
      case 'button': return b.text ? { type: 'button', text: b.text.trim(), href: b.href || '' } : null;
      case 'form':   return b.html ? { type: 'form', html: b.html.replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '') } : null;
      default: return null;
    }
  }).filter(Boolean);

  if (!cleanBlocks.length) return reply.code(400).send({ error: 'No valid blocks provided' });

  const finalId = shortId || uuidv4().slice(0, 8);
  await LandingPage.findOneAndUpdate(
    { shortId: finalId },
    { $set: { userId: req.user.id, title: title.trim(), config: { blocks: cleanBlocks }, updatedAt: new Date() },
      $setOnInsert: { createdAt: new Date() } },
    { upsert: true }
  );

  invalidateUserCache(req.user.id, 'pages');
  invalidatePublicCache(`landing:${finalId}`);
  invalidatePublicCache(`apiPage:${finalId}`);

  reply.send({ success: true, shortId: finalId, url: `\( {req.protocol}:// \){req.hostname}/p/${finalId}` });
});

app.post('/api/pages/delete', { preHandler: app.authenticate }, async (req, reply) => {
  const { shortId } = req.body || {};
  if (!shortId?.trim()) return reply.code(400).send({ error: 'shortId required' });

  const page = await LandingPage.findOne({ shortId, userId: req.user.id });
  if (!page) return reply.code(404).send({ error: 'Page not found' });

  await LandingPage.deleteOne({ shortId });
  invalidateUserCache(req.user.id, 'pages');
  invalidatePublicCache(`landing:${shortId}`);
  invalidatePublicCache(`apiPage:${shortId}`);
  reply.send({ success: true });
});

// ==================== FORMS WRITE ====================
app.post('/api/forms/save', { preHandler: app.authenticate }, async (req, reply) => {
  const { shortId, title, state, welcomeMessage } = req.body || {};
  if (!title?.trim() || !state) return reply.code(400).send({ error: 'title and state required' });

  const limits = getUserLimits(req.user);

  if (!shortId) {
    const count = await FormPage.countDocuments({ userId: req.user.id });
    if (limits.maxForms !== Infinity && count >= limits.maxForms)
      return reply.code(403).send({ error: `Free tier limit: ${limits.maxForms} forms.` });
  } else {
    const existing = await FormPage.findOne({ shortId });
    if (existing && existing.userId !== req.user.id)
      return reply.code(403).send({ error: 'Not your form' });
  }

  const sanitized = JSON.parse(JSON.stringify(state));
  ['headerText','subheaderText','buttonText'].forEach(k => {
    if (sanitized[k]) sanitized[k] = sanitized[k].replace(/<script.*?<\/script>/gi, '');
  });

  const sanitizedWelcome = (welcomeMessage && typeof welcomeMessage === 'string')
    ? sanitizeTelegramHtml(welcomeMessage.trim()) : '';

  const finalId = shortId || uuidv4().slice(0, 8);
  await FormPage.findOneAndUpdate(
    { shortId: finalId },
    { $set: { userId: req.user.id, title: title.trim(), state: sanitized, welcomeMessage: sanitizedWelcome, updatedAt: new Date() },
      $setOnInsert: { createdAt: new Date() } },
    { upsert: true }
  );

  invalidateUserCache(req.user.id, 'forms');
  invalidatePublicCache(`form:${finalId}`);
  invalidatePublicCache(`apiForm:${finalId}`);

  reply.send({ success: true, shortId: finalId, url: `\( {req.protocol}:// \){req.hostname}/f/${finalId}` });
});

app.post('/api/forms/delete', { preHandler: app.authenticate }, async (req, reply) => {
  const { shortId } = req.body || {};
  if (!shortId?.trim()) return reply.code(400).send({ error: 'shortId required' });

  const form = await FormPage.findOne({ shortId, userId: req.user.id });
  if (!form) return reply.code(404).send({ error: 'Form not found' });

  await FormPage.deleteOne({ shortId });
  await Contact.deleteMany({ shortId, userId: req.user.id });
  invalidateUserCache(req.user.id, 'forms');
  invalidatePublicCache(`form:${shortId}`);
  invalidatePublicCache(`apiForm:${shortId}`);
  reply.send({ success: true });
});

// ==================== SUBSCRIBE ====================
app.post('/api/subscribe/:shortId', formSubmitRateLimit, async (req, reply) => {
  const { shortId } = req.params;
  const { name, email } = req.body || {};

  if (!name?.trim())  return reply.code(400).send({ error: 'Name is required' });
  if (!email?.trim()) return reply.code(400).send({ error: 'Contact (email or phone) is required' });

  const contactValue = email.trim();
  if (!CONTACT_REGEX.test(contactValue))
    return reply.code(400).send({ error: 'Contact must be a valid email or phone number' });

  const form = await FormPage.findOne({ shortId });
  if (!form) return reply.code(404).send({ error: 'Form not found' });

  const owner = await User.findOne({ id: form.userId });
  if (!owner?.telegramBotToken || !owner?.botUsername)
    return reply.code(400).send({ error: 'Form owner has not connected a Telegram bot yet' });

  const payload = `sub_\( {shortId}_ \){uuidv4().slice(0, 12)}`;

  let contact = await Contact.findOne({ userId: owner.id, contact: contactValue });

  if (contact) {
    contact.name     = name.trim();
    contact.shortId  = shortId;
    contact.submittedAt = new Date();
    contact.save().catch(() => {});
  } else {
    contact = await Contact.create({
      userId: owner.id, shortId, name: name.trim(), contact: contactValue,
      status: 'pending', submittedAt: new Date(),
    });
  }

  pendingSubscribers.set(payload, {
    userId: owner.id, shortId, name: name.trim(), contact: contactValue, createdAt: Date.now(),
  });

  invalidateUserCache(owner.id, 'contacts');

  const deepLink = `https://t.me/\( {owner.botUsername}?start= \){payload}`;
  const alreadySubscribed = contact.status === 'subscribed';
  reply.send({ success: true, deepLink, alreadySubscribed });
});

// ==================== CONTACTS ====================
app.post('/api/contacts/delete', { preHandler: app.authenticate }, async (req, reply) => {
  const { contacts } = req.body || {};
  if (!Array.isArray(contacts) || !contacts.length)
    return reply.code(400).send({ error: 'Provide a non-empty contacts array' });

  const result = await Contact.deleteMany({ userId: req.user.id, contact: { $in: contacts } });
  invalidateUserCache(req.user.id, 'contacts');
  reply.send({ success: true, deletedCount: result.deletedCount });
});

// ==================== BROADCASTING ====================
app.post('/api/broadcast/now', { preHandler: app.authenticate }, async (req, reply) => {
  const raw = req.body?.message?.trim();
  if (!raw) return reply.code(400).send({ error: 'message required' });
  if (raw.length > MAX_MSG_LENGTH * 10) return reply.code(400).send({ error: 'Message too long' });

  const todayCount = await incrementDailyBroadcast(req.user.id);
  const limits     = getUserLimits(req.user);
  if (limits.dailyBroadcasts !== Infinity && todayCount > limits.dailyBroadcasts)
    return reply.code(403).send({ error: `Daily broadcast limit (${limits.dailyBroadcasts}) reached.` });

  const readyMsg = prepareTelegramMessage(raw);
  if (!readyMsg) return reply.code(400).send({ error: 'Message is empty after processing' });

  await broadcastQueue.add('send-broadcast', { userId: req.user.id, message: readyMsg }, {
    attempts: 4, backoff: { type: 'exponential', delay: 5000 },
  });

  reply.send({ success: true, message: 'Broadcast queued. You will receive a Telegram delivery report shortly.' });
});

app.post('/api/broadcast/schedule', { preHandler: app.authenticate }, async (req, reply) => {
  const { message, scheduledTime, recipients = 'all' } = req.body || {};
  const raw = message?.trim();
  if (!raw) return reply.code(400).send({ error: 'message required' });
  if (raw.length > MAX_MSG_LENGTH * 10) return reply.code(400).send({ error: 'Message too long' });

  const time = scheduledTime ? new Date(scheduledTime) : null;
  if (!time || isNaN(time.getTime()) || time <= new Date())
    return reply.code(400).send({ error: 'scheduledTime must be a valid future datetime' });

  const todayCount = await incrementDailyBroadcast(req.user.id);
  const limits     = getUserLimits(req.user);
  if (limits.dailyBroadcasts !== Infinity && todayCount > limits.dailyBroadcasts)
    return reply.code(403).send({ error: `Daily broadcast limit (${limits.dailyBroadcasts}) reached.` });

  const readyMsg    = prepareTelegramMessage(raw);
  const broadcastId = uuidv4();

  await ScheduledBroadcast.create({
    broadcastId, userId: req.user.id, message: readyMsg, recipients, scheduledTime: time, status: 'pending',
  });

  await broadcastQueue.add('send-broadcast',
    { userId: req.user.id, message: readyMsg, broadcastId },
    { jobId: broadcastId, delay: time.getTime() - Date.now(), attempts: 4, backoff: { type: 'exponential', delay: 5000 } }
  );

  reply.send({ success: true, broadcastId, scheduledTime: time.toISOString() });
});

app.get('/api/broadcast/scheduled', { preHandler: app.authenticate }, async (req, reply) => {
  const list = await ScheduledBroadcast.find({ userId: req.user.id, status: 'pending' }).sort({ scheduledTime: 1 });
  reply.send({
    success: true,
    scheduled: list.map(s => ({
      broadcastId:   s.broadcastId,
      message:       s.message.slice(0, 100) + (s.message.length > 100 ? '…' : ''),
      scheduledTime: s.scheduledTime.toISOString(),
      status:        s.status,
      recipients:    s.recipients,
    })),
  });
});

app.delete('/api/broadcast/scheduled/:broadcastId', { preHandler: app.authenticate }, async (req, reply) => {
  const { broadcastId } = req.params;
  const task = await ScheduledBroadcast.findOne({ broadcastId, userId: req.user.id });
  if (!task) return reply.code(404).send({ error: 'Scheduled broadcast not found' });

  const job = await broadcastQueue.getJob(broadcastId);
  if (job) await job.remove().catch(() => {});
  await task.deleteOne();

  reply.send({ success: true });
});

app.patch('/api/broadcast/scheduled/:broadcastId', { preHandler: app.authenticate }, async (req, reply) => {
  const { broadcastId } = req.params;
  const { message, scheduledTime, recipients } = req.body || {};

  const task = await ScheduledBroadcast.findOne({ broadcastId, userId: req.user.id, status: 'pending' });
  if (!task) return reply.code(400).send({ error: 'Broadcast not found or not editable' });

  const oldJob = await broadcastQueue.getJob(broadcastId);
  if (oldJob) await oldJob.remove().catch(() => {});

  if (message?.trim()) {
    if (message.length > MAX_MSG_LENGTH * 10) return reply.code(400).send({ error: 'Message too long' });
    task.message = prepareTelegramMessage(message.trim());
  }
  if (recipients) task.recipients = recipients;
  if (scheduledTime) {
    const t = new Date(scheduledTime);
    if (isNaN(t.getTime()) || t <= new Date()) return reply.code(400).send({ error: 'Invalid future time' });
    task.scheduledTime = t;
  }

  await task.save();

  await broadcastQueue.add('send-broadcast',
    { userId: task.userId, message: task.message, broadcastId: task.broadcastId },
    { jobId: task.broadcastId, delay: Math.max(0, task.scheduledTime.getTime() - Date.now()), attempts: 4, backoff: { type: 'exponential', delay: 5000 } }
  );

  reply.send({ success: true, broadcastId: task.broadcastId, scheduledTime: task.scheduledTime.toISOString() });
});

app.get('/api/broadcast/scheduled/:broadcastId/details', { preHandler: app.authenticate }, async (req, reply) => {
  const task = await ScheduledBroadcast.findOne({ broadcastId: req.params.broadcastId, userId: req.user.id, status: 'pending' });
  if (!task) return reply.code(404).send({ error: 'Broadcast not found or not editable' });

  const d      = new Date(task.scheduledTime);
  const local  = new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16);

  reply.send({ success: true, message: task.message, scheduledTime: local, recipients: task.recipients || 'all' });
});

// ==================== ADMIN PANEL ====================
function adminHtml(cache, totalUsers, payingUsers) {
  return `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Server Admin Panel</title>
<style>
  body{font-family:'Segoe UI',sans-serif;background:#121212;color:#e0e0e0;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0}
  .wrap{background:#1e1e1e;padding:40px;border-radius:12px;box-shadow:0 8px 32px rgba(0,0,0,.6);width:90%;max-width:600px}
  h1{text-align:center;color:#ffd700;margin-bottom:30px}
  .grid{display:grid;grid-template-columns:1fr 1fr;gap:15px;margin-bottom:30px}
  .stat{background:#2d2d2d;padding:20px;border-radius:10px;text-align:center}
  .num{font-size:2.5em;font-weight:700;color:#00ff41;margin:10px 0}
  label{display:block;margin:20px 0 8px;font-size:1.1em}
  input{width:100%;padding:12px;background:#2d2d2d;border:none;border-radius:6px;color:#fff;font-size:1em;margin-bottom:15px;box-sizing:border-box}
  button{width:100%;padding:14px;background:#ffd700;color:#000;font-weight:700;border:none;border-radius:6px;cursor:pointer;font-size:1.1em;margin-top:20px}
  button:hover{background:#e6c200}
  .cur{text-align:center;margin:25px 0;padding:15px;background:#2d2d2d;border-radius:8px;font-size:1.1em}
</style></head><body>
<div class="wrap">
  <h1>⚙️ Server Admin Panel</h1>
  <div class="grid">
    <div class="stat"><div class="num">${totalUsers}</div><div>Total Users</div></div>
    <div class="stat"><div class="num">${payingUsers}</div><div>Paying Users</div></div>
  </div>
  <form method="POST" action="/admin-limits">
    <label>Owner Password</label>
    <input type="password" name="password" required placeholder="Admin password">
    <label>Daily Broadcasts per Free User</label>
    <input type="number" name="daily_broadcast" min="1" value="${cache.dailyBroadcastLimit}" required>
    <label>Max Landing Pages per Free User</label>
    <input type="number" name="max_pages" min="1" value="${cache.maxLandingPages}" required>
    <label>Max Forms per Free User</label>
    <input type="number" name="max_forms" min="1" value="${cache.maxForms}" required>
    <div class="cur"><strong>Current Free Limits:</strong><br>
      Broadcasts/day: ${cache.dailyBroadcastLimit} | Pages: ${cache.maxLandingPages} | Forms: ${cache.maxForms}
    </div>
    <button type="submit">Update Limits</button>
  </form>
</div></body></html>`;
}

app.get('/admin-limits', async (req, reply) => {
  const [totalUsers, payingUsers] = await Promise.all([
    User.countDocuments(),
    User.countDocuments({ isSubscribed: true, subscriptionEndDate: { $gt: new Date() } }),
  ]);
  reply.type('text/html').send(adminHtml(adminSettingsCache, totalUsers, payingUsers));
});

app.post('/admin-limits', async (req, reply) => {
  const { password, daily_broadcast, max_pages, max_forms } = req.body || {};

  if (password !== ADMIN_PASSWORD)
    return reply.type('text/html').send('<html><body style="background:#121212;color:#f44336;font-family:sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;text-align:center"><h1>Access Denied<br>Wrong Password</h1></body></html>');

  const newDaily = parseInt(daily_broadcast, 10);
  const newPages = parseInt(max_pages, 10);
  const newForms = parseInt(max_forms, 10);

  if ([newDaily, newPages, newForms].some(v => isNaN(v) || v < 1))
    return reply.type('text/html').send('<html><body style="background:#121212;color:#f44336;font-family:sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;text-align:center"><h1>Invalid Values<br>All limits must be ≥ 1</h1></body></html>');

  await AdminSettings.updateSettings({ dailyBroadcastLimit: newDaily, maxLandingPages: newPages, maxForms: newForms });
  adminSettingsCache = { dailyBroadcastLimit: newDaily, maxLandingPages: newPages, maxForms: newForms };

  app.log.info('Admin limits updated:', adminSettingsCache);

  reply.type('text/html').send(`<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>Updated</title>
<style>body{font-family:'Segoe UI',sans-serif;background:#121212;color:#e0e0e0;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0}
.wrap{background:#1e1e1e;padding:40px;border-radius:12px;text-align:center}h1{color:#4caf50}
a{color:#ffd700;text-decoration:none;font-weight:700}a:hover{text-decoration:underline}</style></head>
<body><div class="wrap"><h1>✅ Success!</h1>
<p>Limits updated and saved permanently:</p>
<p><strong>Daily Broadcasts:</strong> ${newDaily}<br>
   <strong>Max Pages:</strong> ${newPages}<br>
   <strong>Max Forms:</strong> ${newForms}</p>
<p><a href="/admin-limits">← Back to Control Panel</a></p></div></body></html>`);
});

// ==================== HEALTH ====================
app.get('/ping', (req, reply) => reply.type('text/plain').send('ok'));

// ==================== GLOBAL ERROR HANDLER ====================
app.setErrorHandler((err, req, reply) => {
  app.log.error(err);
  const code = err.statusCode || 500;
  const msg  = code < 500 ? err.message : 'Internal server error';
  reply.code(code).send({ error: msg });
});

// ==================== 404 HANDLER ====================
app.setNotFoundHandler((req, reply) => {
  if (req.url.startsWith('/api/')) return reply.code(404).send({ error: 'Not found' });
  reply.code(404).view('404');
});

// ==================== STARTUP ====================
async function start() {
  try {
    await new Promise((resolve, reject) => {
      if (mongoose.connection.readyState === 1) return resolve();
      mongoose.connection.once('open', resolve);
      mongoose.connection.once('error', reject);
    });

    const settings = await AdminSettings.getSettings();
    adminSettingsCache = {
      dailyBroadcastLimit: settings.dailyBroadcastLimit,
      maxLandingPages:     settings.maxLandingPages,
      maxForms:            settings.maxForms,
    };
    console.log('✅ Admin settings loaded:', adminSettingsCache);

    const usersWithBots = await User.find({ telegramBotToken: { $exists: true, $ne: null } });
    for (const user of usersWithBots) launchUserBot(user);
    console.log(`✅ Launched ${usersWithBots.length} bot(s) in webhook mode`);

    await recoverScheduledBroadcasts();

    await app.listen({ port: PORT, host: '0.0.0.0' });
    console.log(`\n🚀 SENDM SERVER — Fastify + BullMQ + Redis`);
    console.log(`   Listening on port \( {PORT} | Domain: https:// \){DOMAIN}\n`);
  } catch (err) {
    console.error('Startup error:', err);
    process.exit(1);
  }
}

// ==================== GRACEFUL SHUTDOWN ====================
async function shutdown(signal) {
  console.log(`\n${signal} received — shutting down gracefully…`);
  try {
    await worker.close();
    await broadcastQueue.close();
    await redisConnection.quit();
    await app.close();
    await mongoose.disconnect();
    console.log('✅ Clean shutdown.');
    process.exit(0);
  } catch (err) {
    console.error('Shutdown error:', err);
    process.exit(1);
  }
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('uncaughtException',  (err) => { console.error('Uncaught exception:', err);  process.exit(1); });
process.on('unhandledRejection', (err) => { console.error('Unhandled rejection:', err); process.exit(1); });

start();
