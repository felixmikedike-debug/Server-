require('dotenv').config();

const express = require('express');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const rateLimit = require('express-rate-limit');
const { Telegraf } = require('telegraf');
const path = require('path');
const mongoose = require('mongoose');
const axios = require('axios');
const crypto = require('crypto');
const IORedis = require('ioredis');
const { Queue, Worker } = require('bullmq');

// Bump this string any time you deploy a fix and want an unambiguous way
// to confirm from the OUTSIDE (via /ping or the boot logs) that the
// server actually running is the version you think it is.
const BUILD_TAG = 'simple-computed-bot-assignment-2026-07-14';

const app = express();
console.log('=== BUILD TAG: ' + BUILD_TAG + ' ===');
const PORT = process.env.PORT || 3000;
app.set('trust proxy', 3);

// Flips to true only after Mongo is connected AND the bot pool has
// finished initializing (all bots verified via getMe + webhooks set).
// /ping reports this, and subscribe/broadcast routes refuse to run
// against a half-initialized pool instead of crashing on undefined bots.
let serverReady = false;

// ==================== CONFIG & SECRETS ====================
const JWT_SECRET = process.env.JWT_SECRET || 'fallback_weak_secret_change_me_immediately';
const PAYSTACK_SECRET_KEY = process.env.PAYSTACK_SECRET_KEY || 'sk_test_fallback_change_me';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'midas';
const DOMAIN = process.env.DOMAIN;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;

if (!DOMAIN) {
  console.error('ERROR: DOMAIN environment variable is required for webhooks!');
  process.exit(1);
}

// Unlike the old per-user-bot version, we do NOT auto-generate a
// fallback WEBHOOK_SECRET. In a multi-instance deploy a per-process
// random secret breaks routing (each instance would register a
// different webhook path with Telegram). Fail loud instead.
if (!WEBHOOK_SECRET || !WEBHOOK_SECRET.trim()) {
  console.error('ERROR: WEBHOOK_SECRET must be set in env and stable across restarts/instances.');
  process.exit(1);
}

if (JWT_SECRET.includes('fallback')) {
  console.warn('⚠️  WARNING: JWT_SECRET not set in .env! Using insecure fallback.');
}
if (PAYSTACK_SECRET_KEY.startsWith('sk_test_fallback')) {
  console.warn('⚠️  WARNING: PAYSTACK_SECRET_KEY not set in .env!');
}

const MONTHLY_PRICE_KOBO = 150000; // ₦5,000 in kobo
const CONTACT_REGEX = /^(\+?[0-9\s\-\(\)]{7,20}|[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$/;
const MAX_MSG_LENGTH = 4000;
const BATCH_SIZE = 25;
const BATCH_INTERVAL_MS = 8000;

// ==================== REDIS + BULLMQ ====================
let redisConnection;
if (process.env.REDIS_URL) {
  redisConnection = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: null, enableReadyCheck: false });
} else {
  console.warn('⚠️ WARNING: REDIS_URL not set in .env, falling back to localhost:6379');
  redisConnection = new IORedis({ host: 'localhost', port: 6379, maxRetriesPerRequest: null, enableReadyCheck: false });
}
const broadcastQueue = new Queue('telegram-broadcasts', { connection: redisConnection });

// ==================== MONGODB CONNECTION ====================
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/sendm';
console.log('Connecting to MongoDB:', MONGODB_URI.replace(/:([^:@]+)@/, ':****@'));

mongoose.connect(MONGODB_URI, {
  serverSelectionTimeoutMS: 30000,
  socketTimeoutMS: 45000,
  connectTimeoutMS: 30000,
}).then(() => {
  console.log('✅ MongoDB connected');
}).catch(err => {
  console.error('MongoDB connection failed:', err.message);
  process.exit(1);
});

// ==================== SCHEMAS & MODELS ====================
const userSchema = new mongoose.Schema({
  id: { type: String, required: true, unique: true },
  fullName: String,
  email: { type: String, required: true, unique: true, lowercase: true },
  password: String,
  // Auth 2FA is bound to the single shared auth bot, not a per-user bot.
  telegramChatId: String,
  isTelegramConnected: { type: Boolean, default: false },
  isSubscribed: { type: Boolean, default: false },
  subscriptionEndDate: Date,
  subscriptionPlan: String,
  pendingPaymentReference: String,
  createdAt: { type: Date, default: Date.now },
}, { timestamps: true });

const landingPageSchema = new mongoose.Schema({
  shortId: { type: String, required: true, unique: true },
  userId: { type: String, required: true },
  title: String,
  config: Object,
}, { timestamps: true });

const formPageSchema = new mongoose.Schema({
  shortId: { type: String, required: true, unique: true },
  userId: { type: String, required: true },
  title: String,
  state: Object,
  welcomeMessage: String,
}, { timestamps: true });

const contactSchema = new mongoose.Schema({
  userId: { type: String, required: true },       // the Sendm creator who owns this subscriber
  shortId: String,                                  // which form they subscribed via
  name: String,
  contact: { type: String, required: true },        // email or phone, used for bot assignment + identity
  telegramChatId: String,
  // Telegram @username, captured at /start confirm time (ctx.from.username).
  // Optional field on Telegram's side — many users won't have one, so this
  // can be null. Convenience/display only; never used for identity or
  // bot assignment (telegramChatId + contact remain the real identity
  // anchors), since usernames can change at any time.
  telegramUsername: String,
  status: { type: String, default: 'pending' },
  submittedAt: Date,
  subscribedAt: Date,
  unsubscribedAt: Date,
}, { timestamps: true });

// A subscription link the user hasn't confirmed via /start yet.
// Previously an in-memory Map (lost on restart) — now a real
// TTL-indexed document so it survives restarts and multi-instance
// deployments on Render.
const pendingSubscriberSchema = new mongoose.Schema({
  payload: { type: String, required: true, unique: true }, // 'sub_<shortId>_<uuid>'
  userId: { type: String, required: true },
  shortId: String,
  name: String,
  contact: { type: String, required: true },
  createdAt: { type: Date, default: Date.now, expires: 1800 } // 30 min TTL
});

const scheduledBroadcastSchema = new mongoose.Schema({
  broadcastId: { type: String, required: true, unique: true },
  userId: { type: String, required: true },
  message: String,
  recipients: { type: String, default: 'all' },
  scheduledTime: Date,
  status: { type: String, default: 'pending' },
}, { timestamps: true });

const broadcastDailySchema = new mongoose.Schema({
  userId: { type: String, required: true },
  date: { type: String, required: true },
  count: { type: Number, default: 1 },
}, { timestamps: true });

const adminSettingsSchema = new mongoose.Schema({
  dailyBroadcastLimit: { type: Number, default: 3, min: 1 },
  maxLandingPages: { type: Number, default: 5, min: 1 },
  maxForms: { type: Number, default: 5, min: 1 },
}, { timestamps: true });

adminSettingsSchema.statics.getSettings = async function() {
  let settings = await this.findOne();
  if (!settings) settings = await this.create({});
  return settings;
};
adminSettingsSchema.statics.updateSettings = async function(updates) {
  let settings = await this.findOne();
  if (!settings) settings = new this();
  Object.assign(settings, updates);
  await settings.save();
  return settings;
};

const AdminSettings = mongoose.model('AdminSettings', adminSettingsSchema);
const User = mongoose.model('User', userSchema);
const LandingPage = mongoose.model('LandingPage', landingPageSchema);
const FormPage = mongoose.model('FormPage', formPageSchema);
const Contact = mongoose.model('Contact', contactSchema);
const PendingSubscriber = mongoose.model('PendingSubscriber', pendingSubscriberSchema);
const ScheduledBroadcast = mongoose.model('ScheduledBroadcast', scheduledBroadcastSchema);
const BroadcastDaily = mongoose.model('BroadcastDaily', broadcastDailySchema);

// Indexes
contactSchema.index({ userId: 1 });
contactSchema.index({ userId: 1, contact: 1 });
contactSchema.index({ userId: 1, status: 1 });
contactSchema.index({ status: 1 }); // broadcast worker fans out by status
landingPageSchema.index({ userId: 1 });
formPageSchema.index({ userId: 1 });
scheduledBroadcastSchema.index({ userId: 1 });
scheduledBroadcastSchema.index({ status: 1 });
scheduledBroadcastSchema.index({ scheduledTime: 1 });
broadcastDailySchema.index({ userId: 1, date: 1 }, { unique: true });

// ==================== IN-MEMORY (auth-only, small scale) ====================
const resetTokens = new Map();

// ==================== PER-USER & PUBLIC CACHE WITH TTL ====================
const userCache = new Map();
const publicCache = new Map();

const TTL = {
  pages: 5 * 60 * 1000,
  forms: 5 * 60 * 1000,
  contacts: 2 * 60 * 1000,
  public: 10 * 60 * 1000
};

function getUserCache(userId) {
  let bucket = userCache.get(userId);
  if (!bucket) {
    bucket = { pages: null, forms: null, contacts: null, pagesTs: 0, formsTs: 0, contactsTs: 0, lastAccess: Date.now() };
    userCache.set(userId, bucket);
  } else {
    bucket.lastAccess = Date.now();
  }
  return bucket;
}

function invalidateUserCache(userId, type = 'all') {
  const bucket = userCache.get(userId);
  if (!bucket) return;
  if (type === 'pages' || type === 'all') { bucket.pages = null; bucket.pagesTs = 0; }
  if (type === 'forms' || type === 'all') { bucket.forms = null; bucket.formsTs = 0; }
  if (type === 'contacts' || type === 'all') { bucket.contacts = null; bucket.contactsTs = 0; }
  bucket.lastAccess = Date.now();
}

function invalidatePublicCache(key) {
  publicCache.delete(key);
}

setInterval(() => {
  const now = Date.now();
  const INACTIVE_THRESHOLD = 30 * 60 * 1000;

  for (const [key, val] of publicCache.entries()) {
    if (now - val.timestamp > TTL.public) publicCache.delete(key);
  }
  for (const [userId, bucket] of userCache.entries()) {
    if (now - bucket.lastAccess > INACTIVE_THRESHOLD) {
      userCache.delete(userId);
      console.log('🧹 Cleaned cache for inactive user: ' + userId);
    }
  }
}, 10 * 60 * 1000);

let adminSettingsCache = { dailyBroadcastLimit: 3, maxLandingPages: 5, maxForms: 5 };

// ==================== SHARED BOT POOL ====================
/**
 * Shared platform-owned bot pool.
 *
 * - ONE auth bot: handles 2FA/login codes only. Dedicated token.
 * - N broadcast bots: shared workhorses that deliver subscriber content.
 *   Configured via BROADCAST_BOT_TOKENS (comma-separated). Add more
 *   tokens to that env var any time to scale out — no code changes.
 *
 * Bot assignment: SIMPLE DESIGN. Nothing is stored on the Contact/
 * PendingSubscriber documents. Every time we need to know which bot a
 * contact talks to, we compute it fresh: md5(contact) % poolSize. Same
 * contact + same pool size always gives the same bot, so a subscriber
 * consistently lands on the same bot without us ever persisting or
 * validating a stale index. This removes the whole class of "botKey is
 * undefined/stale/out of range" bugs the stored-key version had.
 *
 * CAVEAT: if you resize the pool (add/remove BROADCAST_BOT_TOKENS)
 * while someone is mid-flow (subscribed but hasn't clicked the Telegram
 * link yet), they could get a different bot at confirm time than at
 * subscribe time. Low-stakes at this scale — just avoid resizing the
 * pool during a big active campaign, and existing SUBSCRIBED contacts
 * are unaffected since sends look up telegramChatId directly.
 */
const botPool = {
  authBot: null,
  broadcastBots: [], // array of { bot, index, token, username }

  get poolSize() {
    return this.broadcastBots.length;
  },

  // Returns the { bot, index, token, username } entry this contact
  // should use, computed fresh — never throws on empty pool, returns
  // null instead so callers can give a clean "try again" response.
  getBotForContact(contactValue) {
    if (this.poolSize === 0) return null;
    const hash = crypto.createHash('md5').update(String(contactValue).toLowerCase().trim()).digest('hex');
    const n = parseInt(hash.slice(0, 8), 16);
    const idx = n % this.poolSize;
    return this.broadcastBots[idx] || null;
  },

  getBroadcastBotByIndex(index) {
    return this.broadcastBots[index] || null;
  },
};

async function getMeWithRetry(bot, label, maxAttempts = 5) {
  let attempts = 0;
  while (attempts < maxAttempts) {
    attempts++;
    try {
      return await bot.telegram.getMe();
    } catch (err) {
      console.warn(label + ' getMe attempt ' + attempts + '/' + maxAttempts + ' failed: ' + err.message);
      if (attempts >= maxAttempts) throw err;
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

async function initBotPool() {
  const authToken = process.env.AUTH_BOT_TOKEN;
  if (!authToken) throw new Error('AUTH_BOT_TOKEN is required — the auth bot is not optional.');

  const rawBroadcastTokens = (process.env.BROADCAST_BOT_TOKENS || '')
    .split(',').map(t => t.trim()).filter(Boolean);

  if (rawBroadcastTokens.length === 0) {
    throw new Error('BROADCAST_BOT_TOKENS is required — need at least 1 broadcast bot, recommend 3-4.');
  }

  // ---- Auth bot ----
  const authBot = new Telegraf(authToken);
  authBot.webhookReply = false;
  authBot.options.webhookReply = false;
  authBot.catch((err) => console.error('Auth bot error:', err.message));

  const authInfo = await getMeWithRetry(authBot, 'auth bot');
  authBot.username = authInfo.username;
  botPool.authBot = authBot;
  console.log('Auth bot ready: @' + authInfo.username);

  // ---- Broadcast bot pool ----
  for (let i = 0; i < rawBroadcastTokens.length; i++) {
    const token = rawBroadcastTokens[i];
    const bot = new Telegraf(token);
    bot.webhookReply = false;
    bot.options.webhookReply = false;
    bot.catch((err) => console.error('Broadcast bot[' + i + '] error:', err.message));

    const info = await getMeWithRetry(bot, 'broadcast bot[' + i + ']');
    botPool.broadcastBots.push({ bot, index: i, token, username: info.username });
    console.log('Broadcast bot[' + i + '] ready: @' + info.username);
  }

  console.log('Bot pool initialized: 1 auth bot + ' + botPool.broadcastBots.length + ' broadcast bots');
}

// ==================== WEBHOOK SETUP (once per bot, at boot) ====================
async function setWebhookWithRetry(bot, url, label, maxAttempts = 5) {
  let attempts = 0;
  while (attempts < maxAttempts) {
    attempts++;
    try {
      const current = await bot.telegram.getWebhookInfo();
      if (current.url === url && current.pending_update_count < 50) {
        console.log('Webhook already correct for ' + label);
        return;
      }

      await bot.telegram.deleteWebhook({ drop_pending_updates: false });

      const ok = await bot.telegram.setWebhook(url, {
        allowed_updates: ['message', 'callback_query', 'my_chat_member']
      });

      if (ok) {
        console.log('Webhook set for ' + label + ' -> ' + url);
        return;
      }
    } catch (err) {
      if (err.response && err.response.error_code === 429) {
        const retryAfter = (err.response.parameters && err.response.parameters.retry_after) || 30;
        console.warn(label + ' rate limited, waiting ' + (retryAfter + 5) + 's');
        await new Promise(r => setTimeout(r, (retryAfter + 5) * 1000));
        continue;
      }
      console.error(label + ' webhook attempt ' + attempts + ' failed: ' + err.message);
      if (attempts >= maxAttempts) throw err;
      await new Promise(r => setTimeout(r, 5000));
    }
  }
  throw new Error('Gave up setting webhook for ' + label + ' after ' + maxAttempts + ' attempts');
}

async function setupAllWebhooks() {
  const authUrl = 'https://' + DOMAIN + '/webhook/auth/' + WEBHOOK_SECRET;
  await setWebhookWithRetry(botPool.authBot, authUrl, 'auth bot');

  for (const entry of botPool.broadcastBots) {
    const url = 'https://' + DOMAIN + '/webhook/broadcast/' + entry.index + '/' + WEBHOOK_SECRET;
    await setWebhookWithRetry(entry.bot, url, 'broadcast bot[' + entry.index + '] (@' + entry.username + ')');
  }

  console.log('All webhooks registered.');
}

// ==================== TEXT / HTML UTILITIES ====================
function escapeHtml(unsafe) {
  if (!unsafe) unsafe = '';
  return unsafe.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}

function sanitizeTelegramHtml(unsafe) {
  if (!unsafe || typeof unsafe !== 'string') return '';
  const allowedTags = new Set(['b','strong','i','em','u','ins','s','strike','del','span','tg-spoiler','a','code','pre','tg-emoji','blockquote']);
  const allowedAttrs = { a: ['href'], 'tg-emoji': ['emoji-id'], 'blockquote': ['expandable'] };

  let clean = unsafe
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
    .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, '')
    .replace(/on\w+="[^"]*"/gi, '')
    .replace(/javascript:/gi, '');

  clean = clean.replace(/<\/?([a-z][a-z0-9]*)\b[^>]*>/gi, function(match, tagName) {
    const tag = tagName.toLowerCase();
    if (!allowedTags.has(tag)) return '';
    if (match.startsWith('</')) return '</' + tag + '>';

    let attrs = '';
    const attrRegex = /([a-z0-9-]+)="([^"]*)"/gi;
    let attrMatch;
    while ((attrMatch = attrRegex.exec(match)) !== null) {
      const attrName = attrMatch[1].toLowerCase();
      let attrValue = attrMatch[2];
      if (allowedAttrs[tag] && allowedAttrs[tag].includes(attrName)) {
        if (attrName === 'href' && !/^https?:\/\//i.test(attrValue) && !attrValue.startsWith('/')) {
          attrValue = '#';
        }
        attrs += ' ' + attrName + '="' + attrValue.replace(/"/g, '&quot;') + '"';
      }
    }
    return '<' + tag + attrs + '>';
  });
  return clean.trim();
}

function textToHtmlForDisplay(text) {
  if (!text) return '';
  return text.replace(/\n{2,}/g, '</p><p>').replace(/\n/g, '<br>');
}

function prepareTelegramMessage(raw) {
  if (!raw || typeof raw !== 'string') return '';
  let msg = raw.trim();
  msg = msg
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>\s*<p[^>]*>/gi, '\n\n')
    .replace(/<p[^>]*>/gi, '')
    .replace(/<\/p>/gi, '\n')
    .replace(/<div[^>]*>/gi, '\n')
    .replace(/<\/div>/gi, '\n')
    .replace(/\n{3,}/g, '\n\n');
  return sanitizeTelegramHtml(msg);
}

function splitTelegramMessage(text) {
  if (!text) return [];
  const chunks = [];
  let current = '';
  const lines = text.split(/\r?\n/);

  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    while (line.length > MAX_MSG_LENGTH) {
      if (current) { chunks.push(current.trim()); current = ''; }
      chunks.push(line.substring(0, MAX_MSG_LENGTH).trim());
      line = line.substring(MAX_MSG_LENGTH);
    }
    if (current.length + line.length + (current ? 1 : 0) <= MAX_MSG_LENGTH) {
      current += (current ? '\n' : '') + line;
    } else {
      if (current) chunks.push(current.trim());
      current = line;
    }
  }
  if (current) chunks.push(current.trim());

  if (chunks.length <= 1) return chunks;
  const total = chunks.length;
  return chunks.map(function(chunk, i) {
    const header = '(' + (i + 1) + '/' + total + ')\n\n';
    return header.length + chunk.length > MAX_MSG_LENGTH ? chunk : header + chunk;
  });
}

function getTodayDateString() {
  return new Date().toISOString().slice(0, 10);
}

function hasActiveSubscription(user) {
  return user.isSubscribed && user.subscriptionEndDate && new Date(user.subscriptionEndDate) > new Date();
}

function getUserLimits(user) {
  if (hasActiveSubscription(user)) return { dailyBroadcasts: Infinity, maxLandingPages: Infinity, maxForms: Infinity };
  return {
    dailyBroadcasts: adminSettingsCache.dailyBroadcastLimit,
    maxLandingPages: adminSettingsCache.maxLandingPages,
    maxForms: adminSettingsCache.maxForms
  };
}

async function incrementDailyBroadcast(userId) {
  const today = getTodayDateString();
  const record = await BroadcastDaily.findOneAndUpdate(
    { userId, date: today }, { $inc: { count: 1 } }, { upsert: true, new: true }
  );
  return record.count;
}

function generate2FACode() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

async function send2FACodeViaBot(user, code) {
  if (!user.isTelegramConnected || !user.telegramChatId) return false;
  try {
    await botPool.authBot.telegram.sendMessage(
      user.telegramChatId,
      'Security Alert – Password Reset\n\nYour 6-digit code:\n\n<b>' + code + '</b>\n\nValid for 10 minutes.',
      { parse_mode: 'HTML' }
    );
    return true;
  } catch (err) {
    console.error('Failed to send 2FA code:', err.message);
    return false;
  }
}

// ==================== BOT HANDLERS ====================
/**
 * Auth bot: every Sendm user talks to the SAME shared bot. We tell them
 * apart via the /start payload (their userId) — same trick as the old
 * per-user model, just no longer needing a distinct bot instance.
 */
function registerAuthBotHandlers(authBot) {
  authBot.start(async (ctx) => {
    const payload = ctx.startPayload || '';
    const chatId = ctx.chat.id.toString();

    if (!payload) {
      await ctx.replyWithHTML('<b>Sendm Security Bot</b>\n\nUse the connect link from your Sendm dashboard to link 2FA.');
      return;
    }

    const user = await User.findOne({ id: payload });
    if (!user) {
      await ctx.replyWithHTML('<b>Invalid or expired link.</b>\n\nPlease generate a new connect link from your dashboard.');
      return;
    }

    user.telegramChatId = chatId;
    user.isTelegramConnected = true;
    await user.save();

    await ctx.replyWithHTML('<b>Sendm 2FA Connected Successfully!</b>\n\nYou will receive login codes here.');
  });

  authBot.command('status', async (ctx) => {
    const chatId = ctx.chat.id.toString();
    const user = await User.findOne({ telegramChatId: chatId, isTelegramConnected: true });
    if (!user) {
      await ctx.replyWithHTML('No Sendm account is linked to this chat.');
      return;
    }
    await ctx.replyWithHTML('<b>Sendm 2FA Status</b>\nAccount: <code>' + user.email + '</code>\nStatus: <b>Connected</b>');
  });
}

/**
 * Wires up ONE broadcast bot's /start handler. Called once per bot in
 * the pool at boot. All broadcast bots share identical logic — the
 * only thing that differs is which token/webhook route they run on,
 * since sharding already guarantees a contact's /start click always
 * lands on the correct bot's webhook.
 */
function registerBroadcastBotHandlers(bot, botIndex) {
  bot.start(async (ctx) => {
    const payload = ctx.startPayload || '';
    const chatId = ctx.chat.id.toString();
    // Telegram usernames are optional and can change at any time —
    // captured here purely for display/convenience, never for identity.
    const tgUsername = ctx.from && ctx.from.username ? ctx.from.username : null;

    if (!payload.startsWith('sub_')) {
      await ctx.replyWithHTML('<b>Welcome!</b>\n\nSubscribe from the page to get updates.');
      return;
    }

    const pending = await PendingSubscriber.findOne({ payload: payload });

    if (!pending) {
      await ctx.replyWithHTML(
        '<b>This subscription link has expired or was already used.</b>\n\n' +
        'Please go back to the page and submit the form again to get a fresh link.'
      );
      return;
    }

    // Note: with the simple design, whichever bot this /start lands on
    // IS the bot this contact will use going forward — we just record
    // telegramChatId, no separate shard-key bookkeeping needed.

    let targetContact = await Contact.findOne({ userId: pending.userId, telegramChatId: chatId });
    const contactsByValue = await Contact.find({ userId: pending.userId, contact: pending.contact });

    if (!targetContact) {
      targetContact = contactsByValue.find(c => c.status === 'subscribed') ||
        contactsByValue.find(c => c.shortId === pending.shortId) ||
        contactsByValue[0];
    }

    if (!targetContact) {
      targetContact = new Contact({
        userId: pending.userId,
        shortId: pending.shortId,
        name: pending.name,
        contact: pending.contact,
        telegramChatId: chatId,
        telegramUsername: tgUsername,
        status: 'subscribed',
        submittedAt: new Date(),
        subscribedAt: new Date()
      });
    } else {
      targetContact.name = pending.name;
      targetContact.contact = pending.contact;
      targetContact.shortId = pending.shortId;
      targetContact.telegramChatId = chatId;
      targetContact.telegramUsername = tgUsername;
      targetContact.status = 'subscribed';
      targetContact.subscribedAt = targetContact.subscribedAt || new Date();
      targetContact.submittedAt = new Date();
    }

    try {
      await targetContact.save();
      await Contact.deleteMany({
        userId: pending.userId,
        $or: [
          { contact: pending.contact, _id: { $ne: targetContact._id } },
          { telegramChatId: chatId, _id: { $ne: targetContact._id } }
        ]
      });
    } catch (err) {
      console.error('Failed to save contact for user ' + pending.userId + ': ' + err.message);
      await ctx.replyWithHTML(
        '<b>Something went wrong saving your subscription.</b>\n\n' +
        'Please try submitting the form again. If this keeps happening, contact support.'
      );
      return;
    }

    await PendingSubscriber.deleteOne({ payload: payload });

    const form = await FormPage.findOne({ shortId: pending.shortId });
    let welcomeText = '<b>Subscription Confirmed!</b>\n\nHi <b>' + escapeHtml(pending.name) + '</b>!\n\nYou\'re now subscribed.\n\nThank you';

    if (form && form.welcomeMessage && form.welcomeMessage.trim()) {
      welcomeText = form.welcomeMessage
        .replace(/\{name\}/gi, '<b>' + escapeHtml(pending.name) + '</b>')
        .replace(/\{contact\}/gi, escapeHtml(pending.contact));
    }

    await ctx.replyWithHTML(welcomeText);
  });
}

// ==================== BROADCAST WORKER ====================
/**
 * Sends `chunks` to a single contact. Which bot to use is computed
 * fresh from the contact's identity value — nothing stored, nothing
 * that can go stale.
 */
async function sendToContact(contact, chunks) {
  const entry = botPool.getBotForContact(contact.contact);
  if (!entry) throw new Error('No broadcast bot available (pool empty) for contact ' + contact.contact);
  for (const chunk of chunks) {
    await entry.bot.telegram.sendMessage(contact.telegramChatId, chunk, { parse_mode: 'HTML' });
  }
}

/**
 * Sends a batch of contacts using whichever bot each one is sharded to.
 * Contacts in a batch can belong to different bots, so sends run
 * concurrently — one contact's send doesn't block another's.
 */
async function sendBatch(batch, chunks, stats) {
  const sendPromises = batch.map(async function(contact) {
    try {
      await sendToContact(contact, chunks);
      stats.sent++;
    } catch (err) {
      stats.failed++;
      const isBlocked = (err.response && err.response.error_code === 403) ||
        /blocked|forbidden|chat not found|deactivated/i.test(err.message || '');
      if (isBlocked) {
        await Contact.findByIdAndUpdate(contact._id, {
          status: 'unsubscribed',
          unsubscribedAt: new Date(),
          telegramChatId: null
        });
      }
    }
  });
  await Promise.all(sendPromises);
}

async function processBroadcast(job) {
  const { userId, message, broadcastId } = job.data;
  const chunks = splitTelegramMessage(message);

  const targets = await Contact.find({
    userId: userId,
    status: 'subscribed',
    telegramChatId: { $exists: true, $ne: null }
  });

  const total = targets.length;
  const stats = { sent: 0, failed: 0 };

  const batches = [];
  for (let i = 0; i < targets.length; i += BATCH_SIZE) {
    batches.push(targets.slice(i, i + BATCH_SIZE));
  }

  for (let b = 0; b < batches.length; b++) {
    await sendBatch(batches[b], chunks, stats);
    if (b < batches.length - 1) {
      await new Promise(resolve => setTimeout(resolve, BATCH_INTERVAL_MS));
    }
  }

  // Delivery report goes to the CREATOR via the auth bot (their 2FA
  // chat), not a broadcast bot — the creator isn't a subscriber shard.
  const user = await User.findOne({ id: userId });
  let reportText = broadcastId ? '<b>Scheduled Broadcast Report</b>\n\n' : '<b>Broadcast Report</b>\n\n';
  if (total === 0) {
    reportText += 'No subscribed contacts with Telegram connected.';
  } else {
    const emoji = stats.failed === 0 ? '✅' : '⚠️';
    reportText += emoji + ' <b>' + stats.sent + ' of ' + total + '</b> delivered.\n';
    if (stats.failed > 0) reportText += stats.failed + ' failed.';
  }
  reportText += '\n\nTime: ' + new Date().toLocaleString();

  if (user && user.isTelegramConnected && user.telegramChatId) {
    try {
      await botPool.authBot.telegram.sendMessage(user.telegramChatId, reportText, { parse_mode: 'HTML' });
    } catch (err) {
      console.error('Failed to send report to user ' + userId, err.message);
    }
  }

  if (broadcastId) {
    await ScheduledBroadcast.deleteOne({ broadcastId: broadcastId });
  }

  invalidateUserCache(userId, 'contacts');
  return stats;
}

const worker = new Worker('telegram-broadcasts', processBroadcast, { connection: redisConnection, concurrency: 4 });

worker.on('completed', function(job) {
  console.log('Broadcast job (' + (job.id || 'immediate') + ') completed for user ' + job.data.userId);
});

worker.on('failed', async function(job, err) {
  console.error('Broadcast job (' + (job.id || 'immediate') + ') failed permanently: ' + err.message);
  const { userId, broadcastId } = job.data || {};
  if (broadcastId) {
    await ScheduledBroadcast.findOneAndUpdate({ broadcastId: broadcastId }, { status: 'failed' }).catch(function() {});
  }
  const user = await User.findOne({ id: userId });
  if (user && user.isTelegramConnected && user.telegramChatId) {
    const text = broadcastId
      ? '<b>Scheduled Broadcast Failed</b>\n\nFailed after retries.\nError: ' + err.message
      : '<b>Broadcast Failed</b>\n\nFailed after retries.\nError: ' + err.message;
    try {
      await botPool.authBot.telegram.sendMessage(user.telegramChatId, text, { parse_mode: 'HTML' });
    } catch {}
  }
});

// ==================== SCHEDULED BROADCAST RECOVERY AFTER RESTART ====================
async function recoverLostScheduledBroadcasts() {
  console.log('🔄 Starting recovery of scheduled broadcasts after server restart...');

  const now = new Date();
  const pendingFuture = await ScheduledBroadcast.find({
    status: 'pending',
    scheduledTime: { $gt: now }
  }).lean();

  if (pendingFuture.length === 0) {
    console.log('✓ No pending future scheduled broadcasts need recovery');
    return;
  }

  console.log('Found ' + pendingFuture.length + ' scheduled broadcast(s) to recover');

  let recovered = 0;
  let alreadyExists = 0;

  for (const task of pendingFuture) {
    const jobId = task.broadcastId;
    const existing = await broadcastQueue.getJob(jobId);
    if (existing) { alreadyExists++; continue; }

    const delayMs = task.scheduledTime.getTime() - Date.now();

    await broadcastQueue.add(
      'send-broadcast',
      { userId: task.userId, message: task.message, broadcastId: task.broadcastId },
      {
        jobId: task.broadcastId,
        delay: delayMs > 1000 ? delayMs : 0,
        attempts: 4,
        backoff: { type: 'exponential', delay: 5000 }
      }
    );

    recovered++;
  }

  console.log('✓ Recovery completed: ' + recovered + ' broadcast(s) re-queued, ' + alreadyExists + ' were already present in queue');
}

// ==================== MIDDLEWARE ====================
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static('public'));
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  message: { error: 'Too many attempts' }
});

const formSubmitLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  message: { error: 'Too many submissions to this form. Please try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: function(req) { return req.ip + '::' + req.params.shortId; },
  skip: function(req) { return !req.params.shortId; }
});

// ==================== WEBHOOK ROUTES (one per bot) ====================
app.post('/webhook/auth/:secret', async (req, res) => {
  if (req.params.secret !== WEBHOOK_SECRET) return res.sendStatus(404);
  try {
    const update = Buffer.isBuffer(req.body) ? JSON.parse(req.body.toString('utf8')) : req.body;
    await botPool.authBot.handleUpdate(update);
  } catch (err) {
    console.error('Auth webhook handling error:', err.message);
  }
  res.sendStatus(200);
});

app.post('/webhook/broadcast/:botIndex/:secret', async (req, res) => {
  if (req.params.secret !== WEBHOOK_SECRET) return res.sendStatus(404);
  const idx = parseInt(req.params.botIndex, 10);
  const entry = botPool.getBroadcastBotByIndex(idx);
  if (!entry) {
    console.warn('Webhook hit for unknown broadcast bot index ' + idx);
    return res.sendStatus(404);
  }
  try {
    const update = Buffer.isBuffer(req.body) ? JSON.parse(req.body.toString('utf8')) : req.body;
    await entry.bot.handleUpdate(update);
  } catch (err) {
    console.error('Broadcast[' + idx + '] webhook handling error:', err.message);
  }
  res.sendStatus(200);
});

// ==================== JWT AUTH ====================
const authenticateToken = async function(req, res, next) {
  const authHeader = req.headers.authorization;
  const token = authHeader && authHeader.startsWith('Bearer ') ? authHeader.split(' ')[1] : req.query.token;

  if (!token) return res.status(401).json({ error: 'Access token required' });

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    const user = await User.findOne({ id: decoded.userId });
    if (!user) return res.status(404).json({ error: 'User not found' });
    req.user = user;
    next();
  } catch (err) {
    res.status(403).json({ error: 'Invalid or expired token' });
  }
};

// ==================== AUTH ROUTES ====================
app.post('/api/auth/register', authLimiter, async function(req, res) {
  const { fullName, email, password } = req.body;
  if (!fullName || !email || !password) return res.status(400).json({ error: 'All fields required' });

  const existing = await User.findOne({ email: email.toLowerCase() });
  if (existing) return res.status(409).json({ error: 'Email already exists' });

  const hashed = await bcrypt.hash(password, 12);
  const newUser = await User.create({
    id: uuidv4(),
    fullName: fullName.trim(),
    email: email.toLowerCase(),
    password: hashed,
  });

  const token = jwt.sign({ userId: newUser.id }, JWT_SECRET, { expiresIn: '7d' });
  res.status(201).json({
    success: true,
    token: token,
    user: { id: newUser.id, fullName: newUser.fullName, email: newUser.email, isTelegramConnected: false }
  });
});

app.post('/api/auth/login', authLimiter, async function(req, res) {
  const { email, password } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'Email and password required' });

  const user = await User.findOne({ email: email.toLowerCase() });
  if (!user || !(await bcrypt.compare(password, user.password))) {
    return res.status(401).json({ error: 'Invalid credentials' });
  }

  const token = jwt.sign({ userId: user.id }, JWT_SECRET, { expiresIn: '7d' });
  res.json({
    success: true,
    token: token,
    user: { id: user.id, fullName: user.fullName, email: user.email, isTelegramConnected: user.isTelegramConnected }
  });
});

app.get('/api/auth/me', authenticateToken, function(req, res) {
  res.json({
    user: {
      id: req.user.id,
      fullName: req.user.fullName,
      email: req.user.email,
      isTelegramConnected: req.user.isTelegramConnected
    }
  });
});

// Connect 2FA: just a deep link into the SHARED auth bot. No token entry
// or validation needed since there's nothing per-user to configure.
app.get('/api/auth/connect-telegram-link', authenticateToken, function(req, res) {
  if (!serverReady || !botPool.authBot) {
    return res.status(503).json({ error: 'Server is still starting up. Please try again in a few seconds.' });
  }
  const startLink = 'https://t.me/' + botPool.authBot.username + '?start=' + req.user.id;
  res.json({ success: true, startLink: startLink, botUsername: '@' + botPool.authBot.username });
});

app.post('/api/auth/disconnect-telegram', authenticateToken, async function(req, res) {
  req.user.telegramChatId = null;
  req.user.isTelegramConnected = false;
  await req.user.save();
  res.json({ success: true, message: 'Telegram 2FA disconnected.' });
});

app.get('/api/auth/bot-status', authenticateToken, function(req, res) {
  res.json({ activated: req.user.isTelegramConnected, chatId: req.user.telegramChatId || null });
});

app.post('/api/auth/forgot-password', async function(req, res) {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: 'Email required' });

  const user = await User.findOne({ email: email.toLowerCase() });
  if (!user) return res.json({ success: true, message: 'If account exists, code was sent.' });
  if (!user.isTelegramConnected) return res.status(400).json({ error: 'Telegram 2FA not connected' });

  const code = generate2FACode();
  const resetToken = uuidv4();
  resetTokens.set(resetToken, { userId: user.id, code: code, expiresAt: Date.now() + 10 * 60 * 1000 });

  const sent = await send2FACodeViaBot(user, code);
  if (!sent) return res.status(500).json({ error: 'Failed to send code' });

  res.json({ success: true, message: 'Code sent!', resetToken: resetToken });
});

app.post('/api/auth/verify-reset-code', function(req, res) {
  const { resetToken, code } = req.body;
  if (!resetToken || !code) return res.status(400).json({ error: 'Token and code required' });

  const entry = resetTokens.get(resetToken);
  if (!entry || Date.now() > entry.expiresAt) {
    resetTokens.delete(resetToken);
    return res.status(400).json({ error: 'Invalid or expired code' });
  }
  if (entry.code !== code.trim()) return res.status(400).json({ error: 'Wrong code' });

  res.json({ success: true, message: 'Verified', userId: entry.userId });
});

app.post('/api/auth/reset-password', async function(req, res) {
  const { resetToken, newPassword } = req.body;
  if (!resetToken || !newPassword || newPassword.length < 6) return res.status(400).json({ error: 'Valid token and password required' });

  const entry = resetTokens.get(resetToken);
  if (!entry || Date.now() > entry.expiresAt) {
    resetTokens.delete(resetToken);
    return res.status(400).json({ error: 'Invalid session' });
  }

  const user = await User.findOne({ id: entry.userId });
  if (!user) return res.status(404).json({ error: 'User not found' });

  user.password = await bcrypt.hash(newPassword, 12);
  await user.save();
  resetTokens.delete(resetToken);

  res.json({ success: true, message: 'Password reset successful' });
});

// ==================== SUBSCRIPTION ROUTES (Paystack, unchanged) ====================
app.get('/api/subscription/status', authenticateToken, async function(req, res) {
  const subscribed = hasActiveSubscription(req.user);
  res.json({
    subscribed: subscribed,
    plan: subscribed ? 'premium-monthly' : 'free',
    endDate: req.user.subscriptionEndDate || null,
    daysLeft: subscribed
      ? Math.ceil((new Date(req.user.subscriptionEndDate) - new Date()) / (1000 * 60 * 60 * 24))
      : 0
  });
});

app.post('/api/subscription/initiate', authenticateToken, async function(req, res) {
  if (hasActiveSubscription(req.user)) {
    return res.status(400).json({ error: 'You already have an active subscription' });
  }

  try {
    const response = await axios.post(
      'https://api.paystack.co/transaction/initialize',
      {
        email: req.user.email,
        amount: MONTHLY_PRICE_KOBO,
        currency: 'NGN',
        callback_url: req.protocol + '://' + req.get('host') + '/subscription-success',
        metadata: { userId: req.user.id, plan: 'premium-monthly' }
      },
      { headers: { Authorization: 'Bearer ' + PAYSTACK_SECRET_KEY, 'Content-Type': 'application/json' } }
    );

    const authorization_url = response.data.data.authorization_url;
    const reference = response.data.data.reference;

    req.user.pendingPaymentReference = reference;
    await req.user.save();

    res.json({ success: true, authorizationUrl: authorization_url, reference: reference });
  } catch (error) {
    console.error('Paystack init error:', error.response ? error.response.data : error.message);
    res.status(500).json({ error: 'Failed to initialize payment' });
  }
});

app.post('/api/subscription/webhook', async function(req, res) {
  try {
    const hash = crypto.createHmac('sha512', PAYSTACK_SECRET_KEY)
      .update(JSON.stringify(req.body))
      .digest('hex');

    if (hash !== req.headers['x-paystack-signature']) {
      return res.status(401).send('Invalid signature');
    }

    const event = req.body;

    if (event.event === 'charge.success') {
      const reference = event.data.reference;
      const userId = event.data.metadata?.userId;

      if (!userId) return res.status(200).send('OK');

      const user = await User.findOne({ id: userId });
      if (!user || user.pendingPaymentReference !== reference) {
        return res.status(200).send('OK');
      }

      const endDate = new Date();
      endDate.setDate(endDate.getDate() + 30);

      user.isSubscribed = true;
      user.subscriptionEndDate = endDate;
      user.subscriptionPlan = 'premium-monthly';
      user.pendingPaymentReference = undefined;
      await user.save();

      console.log('Subscription activated for ' + user.email + ' (ref: ' + reference + ')');
    }

    res.status(200).send('OK');
  } catch (err) {
    console.error('Webhook error:', err);
    res.status(200).send('OK');
  }
});

app.get('/subscription-success', function(req, res) {
  res.send('<!DOCTYPE html>\n<html lang="en">\n<head>\n  <meta charset="UTF-8">\n  <title>Payment Successful</title>\n  <style>\n    body{font-family:system-ui,sans-serif;background:#0a0a0a;color:#00ff41;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;}\n    .box{background:#111;padding:60px;border-radius:20px;text-align:center;box-shadow:0 0 30px rgba(0,255,65,0.2);}\n    h1{margin:0 0 20px;font-size:3em;color:#00ff41;}\n    p{font-size:1.3em;margin:20px 0;line-height:1.6;}\n    a{display:inline-block;margin-top:30px;padding:14px 32px;background:#00ff41;color:#000;font-weight:bold;text-decoration:none;border-radius:8px;font-size:1.1em;}\n    a:hover{background:#00cc33;}\n  </style>\n</head>\n<body>\n  <div class="box">\n    <h1>✓ Payment Successful!</h1>\n    <p>Your subscription is now <strong>active</strong>.</p>\n    <p>You have unlimited broadcasts, landing pages, and forms.</p>\n    <p><a href="https://sendmi.onrender.com">← Return to Dashboard</a></p>\n  </div>\n</body>\n</html>');
});

// ==================== CACHED HIGH-READ ENDPOINTS ====================
app.get('/p/:shortId', async function(req, res) {
  const key = 'landing:' + req.params.shortId;
  const cached = publicCache.get(key);

  if (cached && Date.now() - cached.timestamp < TTL.public) {
    return res.render('landing', cached.data);
  }

  const page = await LandingPage.findOne({ shortId: req.params.shortId });
  if (!page) return res.status(404).render('404');

  const processedBlocks = page.config.blocks.map(function(block) {
    if (block.type === 'text') {
      return { ...block, htmlContent: textToHtmlForDisplay(block.content) };
    }
    return block;
  });

  const data = { title: page.title, blocks: processedBlocks };

  publicCache.set(key, { data: data, timestamp: Date.now() });
  res.render('landing', data);
});

app.get('/f/:shortId', async function(req, res) {
  const key = 'form:' + req.params.shortId;
  const cached = publicCache.get(key);

  if (cached && Date.now() - cached.timestamp < TTL.public) {
    return res.render('form', cached.data);
  }

  const form = await FormPage.findOne({ shortId: req.params.shortId });
  if (!form) return res.status(404).render('404');

  const data = { title: form.title, state: form.state };
  publicCache.set(key, { data: data, timestamp: Date.now() });
  res.render('form', data);
});

app.get('/api/pages', authenticateToken, async function(req, res) {
  const bucket = getUserCache(req.user.id);
  const now = Date.now();

  if (bucket.pages && now - bucket.pagesTs < TTL.pages) {
    return res.json({ pages: bucket.pages });
  }

  const pages = await LandingPage.find({ userId: req.user.id }).sort({ updatedAt: -1 });
  const host = req.get('host');
  const protocol = req.protocol;
  const formatted = pages.map(function(p) {
    return {
      shortId: p.shortId,
      title: p.title,
      createdAt: p.createdAt,
      updatedAt: p.updatedAt,
      url: protocol + '://' + host + '/p/' + p.shortId
    };
  });

  bucket.pages = formatted;
  bucket.pagesTs = now;
  res.json({ pages: formatted });
});

app.get('/api/forms', authenticateToken, async function(req, res) {
  const bucket = getUserCache(req.user.id);
  const now = Date.now();

  if (bucket.forms && now - bucket.formsTs < TTL.forms) {
    return res.json({ forms: bucket.forms });
  }

  const forms = await FormPage.find({ userId: req.user.id }).sort({ updatedAt: -1 });
  const host = req.get('host');
  const protocol = req.protocol;
  const formatted = forms.map(function(f) {
    return {
      shortId: f.shortId,
      title: f.title,
      createdAt: f.createdAt,
      updatedAt: f.updatedAt,
      url: protocol + '://' + host + '/f/' + f.shortId
    };
  });

  bucket.forms = formatted;
  bucket.formsTs = now;
  res.json({ forms: formatted });
});

app.get('/api/contacts', authenticateToken, async function(req, res) {
  const bucket = getUserCache(req.user.id);
  const now = Date.now();

  if (bucket.contacts && now - bucket.contactsTs < TTL.contacts) {
    return res.json({ success: true, contacts: bucket.contacts });
  }

  const contacts = await Contact.find({ userId: req.user.id }).sort({ submittedAt: -1 });
  const formatted = contacts.map(function(c) {
    return {
      name: c.name,
      contact: c.contact,
      status: c.status,
      telegramChatId: c.telegramChatId || null,
      telegramUsername: c.telegramUsername || null,
      pageId: c.shortId,
      submittedAt: new Date(c.submittedAt).toLocaleString(),
      subscribedAt: c.subscribedAt ? new Date(c.subscribedAt).toLocaleString() : null
    };
  });

  bucket.contacts = formatted;
  bucket.contactsTs = now;
  res.json({ success: true, contacts: formatted });
});

app.get('/api/page/:shortId', async function(req, res) {
  const key = 'apiPage:' + req.params.shortId;
  const cached = publicCache.get(key);
  if (cached && Date.now() - cached.timestamp < TTL.public) {
    return res.json(cached.data);
  }

  const page = await LandingPage.findOne({ shortId: req.params.shortId });
  if (!page) return res.status(404).json({ error: 'Page not found' });

  const data = { shortId: page.shortId, title: page.title, config: page.config };
  publicCache.set(key, { data: data, timestamp: Date.now() });
  res.json(data);
});

app.get('/api/form/:shortId', async function(req, res) {
  const key = 'apiForm:' + req.params.shortId;
  const cached = publicCache.get(key);
  if (cached && Date.now() - cached.timestamp < TTL.public) {
    return res.json(cached.data);
  }

  const form = await FormPage.findOne({ shortId: req.params.shortId });
  if (!form) return res.status(404).json({ error: 'Form not found' });

  const data = { shortId: form.shortId, title: form.title, state: form.state, welcomeMessage: form.welcomeMessage };
  publicCache.set(key, { data: data, timestamp: Date.now() });
  res.json(data);
});

// ==================== LANDING PAGES WRITE ROUTES ====================
app.post('/api/pages/save', authenticateToken, async function(req, res) {
  const { shortId, title, config } = req.body;
  if (!title || !config || !Array.isArray(config.blocks)) return res.status(400).json({ error: 'Title and config.blocks required' });

  const limits = getUserLimits(req.user);

  if (!shortId) {
    const currentCount = await LandingPage.countDocuments({ userId: req.user.id });
    if (currentCount >= limits.maxLandingPages && limits.maxLandingPages !== Infinity) {
      return res.status(403).json({ error: 'Maximum landing pages limit reached.' });
    }
  }

  const finalShortId = shortId || uuidv4().slice(0, 8);
  const now = new Date();

  const cleanBlocks = config.blocks.map(function(b) {
    if (!b || b.isEditor || (b.id && (b.id.includes('editor-') || b.id.includes('control-')))) return null;
    if (b.type === 'text') return { type: 'text', tag: b.tag || 'p', content: (b.content || '').trim() };
    if (b.type === 'image') return b.src ? { type: 'image', src: b.src.trim() } : null;
    if (b.type === 'button') return b.text ? { type: 'button', text: b.text.trim(), href: b.href || '' } : null;
    if (b.type === 'form') return b.html ? { type: 'form', html: b.html.replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '') } : null;
    return null;
  }).filter(Boolean);

  if (cleanBlocks.length === 0) return res.status(400).json({ error: 'No valid blocks' });

  await LandingPage.findOneAndUpdate(
    { shortId: finalShortId },
    { userId: req.user.id, title: title.trim(), config: { blocks: cleanBlocks }, updatedAt: now, createdAt: shortId ? undefined : now },
    { upsert: true }
  );

  invalidateUserCache(req.user.id, 'pages');
  invalidatePublicCache('landing:' + finalShortId);
  invalidatePublicCache('apiPage:' + finalShortId);

  const url = req.protocol + '://' + req.get('host') + '/p/' + finalShortId;
  res.json({ success: true, shortId: finalShortId, url: url });
});

app.post('/api/pages/delete', authenticateToken, async function(req, res) {
  const { shortId } = req.body;
  const page = await LandingPage.findOne({ shortId: shortId, userId: req.user.id });
  if (!page) return res.status(404).json({ error: 'Page not found' });
  await LandingPage.deleteOne({ shortId: shortId });

  invalidateUserCache(req.user.id, 'pages');
  invalidatePublicCache('landing:' + shortId);
  invalidatePublicCache('apiPage:' + shortId);

  res.json({ success: true });
});

// ==================== FORMS WRITE ROUTES ====================
app.post('/api/forms/save', authenticateToken, async function(req, res) {
  const { shortId, title, state, welcomeMessage } = req.body;
  if (!title || !state) return res.status(400).json({ error: 'Title and state required' });

  const limits = getUserLimits(req.user);

  if (!shortId) {
    const currentCount = await FormPage.countDocuments({ userId: req.user.id });
    if (currentCount >= limits.maxForms && limits.maxForms !== Infinity) {
      return res.status(403).json({ error: 'Maximum forms limit reached.' });
    }
  }

  const sanitizedState = JSON.parse(JSON.stringify(state));
  if (sanitizedState.headerText) sanitizedState.headerText = sanitizedState.headerText.replace(/<script.*?<\/script>/gi, '');
  if (sanitizedState.subheaderText) sanitizedState.subheaderText = sanitizedState.subheaderText.replace(/<script.*?<\/script>/gi, '');
  if (sanitizedState.buttonText) sanitizedState.buttonText = sanitizedState.buttonText.replace(/<script.*?<\/script>/gi, '');

  const sanitizedWelcome = welcomeMessage && typeof welcomeMessage === 'string'
    ? sanitizeTelegramHtml(welcomeMessage.trim())
    : '';

  const finalShortId = shortId || uuidv4().slice(0, 8);
  const now = new Date();

  await FormPage.findOneAndUpdate(
    { shortId: finalShortId },
    { userId: req.user.id, title: title.trim(), state: sanitizedState, welcomeMessage: sanitizedWelcome, updatedAt: now, createdAt: shortId ? undefined : now },
    { upsert: true }
  );

  invalidateUserCache(req.user.id, 'forms');
  invalidatePublicCache('form:' + finalShortId);
  invalidatePublicCache('apiForm:' + finalShortId);

  const url = req.protocol + '://' + req.get('host') + '/f/' + finalShortId;
  res.json({ success: true, shortId: finalShortId, url: url });
});

app.post('/api/forms/delete', authenticateToken, async function(req, res) {
  const { shortId } = req.body;
  const form = await FormPage.findOne({ shortId: shortId, userId: req.user.id });
  if (!form) return res.status(404).json({ error: 'Form not found' });
  await FormPage.deleteOne({ shortId: shortId });
  await Contact.deleteMany({ shortId: shortId, userId: req.user.id });

  invalidateUserCache(req.user.id, 'forms');
  invalidatePublicCache('form:' + shortId);
  invalidatePublicCache('apiForm:' + shortId);

  res.json({ success: true });
});

// ==================== SUBSCRIBE & CONTACTS ====================
app.post('/api/subscribe/:shortId', formSubmitLimiter, async function(req, res) {
  if (!serverReady || botPool.poolSize === 0) {
    return res.status(503).json({ error: 'Server is still starting up. Please try again in a few seconds.' });
  }

  try {
    const shortId = req.params.shortId;
    const { name, email } = req.body;

    if (!name || !name.trim()) return res.status(400).json({ error: 'Name is required' });
    if (!email || !email.trim()) return res.status(400).json({ error: 'Contact is required' });

    const contactValue = email.trim();

    if (!CONTACT_REGEX.test(contactValue)) {
      return res.status(400).json({ error: 'Contact must be a valid email address or phone number' });
    }

    const form = await FormPage.findOne({ shortId: shortId });
    if (!form) return res.status(404).json({ error: 'Form not found' });

    const owner = await User.findOne({ id: form.userId });
    if (!owner) return res.status(400).json({ error: 'Form owner not found' });

    // Which broadcast bot handles this contact is computed fresh, right
    // here, every time — nothing stored, nothing that can go stale.
    const broadcastBotEntry = botPool.getBotForContact(contactValue);
    if (!broadcastBotEntry) {
      console.error('No broadcast bot available (pool empty) for contact ' + contactValue + ' (owner ' + owner.id + ')');
      return res.status(503).json({ error: 'Unable to assign a broadcast bot right now. Please try again shortly.' });
    }

    const payload = 'sub_' + shortId + '_' + uuidv4().slice(0, 12);

    let contact = await Contact.findOne({ userId: owner.id, contact: contactValue });

    if (contact) {
      if (contact.status === 'subscribed') {
        contact.name = name.trim();
        contact.shortId = shortId;
        contact.submittedAt = new Date();
        await contact.save();

        await PendingSubscriber.create({ payload, userId: owner.id, shortId, name: name.trim(), contact: contactValue });

        const deepLink = 'https://t.me/' + broadcastBotEntry.username + '?start=' + payload;
        return res.json({ success: true, deepLink: deepLink, alreadySubscribed: true });
      }

      contact.name = name.trim();
      contact.shortId = shortId;
      contact.submittedAt = new Date();
    } else {
      contact = new Contact({
        userId: owner.id,
        shortId: shortId,
        name: name.trim(),
        contact: contactValue,
        status: 'pending',
        submittedAt: new Date()
      });
    }
    await contact.save();

    await PendingSubscriber.create({ payload, userId: owner.id, shortId, name: name.trim(), contact: contactValue });

    const deepLink = 'https://t.me/' + broadcastBotEntry.username + '?start=' + payload;
    res.json({ success: true, deepLink: deepLink });

    invalidateUserCache(owner.id, 'contacts');
  } catch (err) {
    console.error('Subscribe error for shortId ' + req.params.shortId + ': ' + err.message);
    res.status(500).json({ error: 'Something went wrong while subscribing. Please try again.' });
  }
});

app.post('/api/contacts/delete', authenticateToken, async function(req, res) {
  const { contacts } = req.body;
  if (!Array.isArray(contacts) || contacts.length === 0) return res.status(400).json({ error: 'Provide contact array' });

  const result = await Contact.deleteMany({ userId: req.user.id, contact: { $in: contacts } });

  invalidateUserCache(req.user.id, 'contacts');

  res.json({ success: true, deletedCount: result.deletedCount });
});

// ==================== BROADCASTING ====================
app.post('/api/broadcast/now', authenticateToken, async function(req, res) {
  if (!serverReady || botPool.poolSize === 0) {
    return res.status(503).json({ error: 'Server is still starting up. Please try again in a few seconds.' });
  }

  const { message } = req.body;
  if (!message || !message.trim()) return res.status(400).json({ error: 'Message required' });

  const processed = message.trim();
  if (processed.length > MAX_MSG_LENGTH * 10) {
    return res.status(400).json({ error: 'Message too long' });
  }

  const todayCount = await incrementDailyBroadcast(req.user.id);
  const limits = getUserLimits(req.user);
  if (todayCount > limits.dailyBroadcasts && limits.dailyBroadcasts !== Infinity) {
    return res.status(403).json({ error: 'Daily broadcast limit reached.' });
  }

  const readyMessage = prepareTelegramMessage(processed);

  if (readyMessage.length === 0) {
    return res.status(400).json({ error: 'Message empty after processing' });
  }

  await broadcastQueue.add('send-broadcast', {
    userId: req.user.id,
    message: readyMessage
  }, {
    attempts: 4,
    backoff: { type: 'exponential', delay: 5000 }
  });

  res.json({
    success: true,
    message: 'Broadcast queued and sending in background. You will receive a delivery report via Telegram shortly.'
  });
});

app.post('/api/broadcast/schedule', authenticateToken, async function(req, res) {
  if (!serverReady || botPool.poolSize === 0) {
    return res.status(503).json({ error: 'Server is still starting up. Please try again in a few seconds.' });
  }

  const { message, scheduledTime, recipients = 'all' } = req.body;
  if (!message || !message.trim()) return res.status(400).json({ error: 'Message required' });

  const processed = message.trim();
  if (processed.length > MAX_MSG_LENGTH * 10) {
    return res.status(400).json({ error: 'Message too long' });
  }

  const todayCount = await incrementDailyBroadcast(req.user.id);
  const limits = getUserLimits(req.user);
  if (todayCount > limits.dailyBroadcasts && limits.dailyBroadcasts !== Infinity) {
    return res.status(403).json({ error: 'Daily broadcast limit reached.' });
  }

  const time = new Date(scheduledTime);
  if (isNaN(time.getTime()) || time <= new Date()) {
    return res.status(400).json({ error: 'Invalid future time' });
  }

  const readyMessage = prepareTelegramMessage(processed);
  const broadcastId = uuidv4();

  await ScheduledBroadcast.create({
    broadcastId: broadcastId,
    userId: req.user.id,
    message: readyMessage,
    recipients: recipients,
    scheduledTime: time,
    status: 'pending'
  });

  const delay = time.getTime() - Date.now();

  await broadcastQueue.add('send-broadcast', {
    userId: req.user.id,
    message: readyMessage,
    broadcastId: broadcastId
  }, {
    jobId: broadcastId,
    delay: delay,
    attempts: 4,
    backoff: { type: 'exponential', delay: 5000 }
  });

  res.json({ success: true, broadcastId: broadcastId, scheduledTime: time.toISOString() });
});

app.get('/api/broadcast/scheduled', authenticateToken, async function(req, res) {
  const scheduled = await ScheduledBroadcast.find({ userId: req.user.id, status: 'pending' }).sort({ scheduledTime: 1 });
  const formatted = scheduled.map(function(s) {
    return {
      broadcastId: s.broadcastId,
      message: s.message.substring(0, 100) + (s.message.length > 100 ? '...' : ''),
      scheduledTime: s.scheduledTime.toISOString(),
      status: s.status,
      recipients: s.recipients
    };
  });
  res.json({ success: true, scheduled: formatted });
});

app.delete('/api/broadcast/scheduled/:broadcastId', authenticateToken, async function(req, res) {
  const broadcastId = req.params.broadcastId;
  const task = await ScheduledBroadcast.findOne({ broadcastId: broadcastId, userId: req.user.id });
  if (!task) return res.status(404).json({ error: 'Not found' });

  const job = await broadcastQueue.getJob(broadcastId);
  if (job) await job.remove();

  await task.deleteOne();

  res.json({ success: true });
});

app.patch('/api/broadcast/scheduled/:broadcastId', authenticateToken, async function(req, res) {
  const { message, scheduledTime, recipients } = req.body;
  const task = await ScheduledBroadcast.findOne({ broadcastId: req.params.broadcastId, userId: req.user.id, status: 'pending' });

  if (!task) return res.status(400).json({ error: 'Cannot edit this broadcast' });

  const oldJob = await broadcastQueue.getJob(task.broadcastId);
  if (oldJob) await oldJob.remove();

  let needsUpdate = false;

  if (message && message.trim()) {
    const processed = message.trim();
    if (processed.length > MAX_MSG_LENGTH * 10) {
      return res.status(400).json({ error: 'Message too long' });
    }
    task.message = prepareTelegramMessage(processed);
    needsUpdate = true;
  }
  if (recipients) {
    task.recipients = recipients;
    needsUpdate = true;
  }
  if (scheduledTime) {
    const newTime = new Date(scheduledTime);
    if (isNaN(newTime.getTime()) || newTime <= new Date()) return res.status(400).json({ error: 'Invalid future time' });
    task.scheduledTime = newTime;
    needsUpdate = true;
  }

  if (needsUpdate) {
    await task.save();

    const delay = task.scheduledTime.getTime() - Date.now();

    await broadcastQueue.add('send-broadcast', {
      userId: task.userId,
      message: task.message,
      broadcastId: task.broadcastId
    }, {
      jobId: task.broadcastId,
      delay: delay > 0 ? delay : 0,
      attempts: 4,
      backoff: { type: 'exponential', delay: 5000 }
    });
  }

  res.json({ success: true, broadcastId: task.broadcastId, scheduledTime: task.scheduledTime.toISOString() });
});

app.get('/api/broadcast/scheduled/:broadcastId/details', authenticateToken, async function(req, res) {
  const task = await ScheduledBroadcast.findOne({ broadcastId: req.params.broadcastId, userId: req.user.id });

  if (!task || task.status !== 'pending') {
    return res.status(404).json({ error: 'Broadcast not found or not editable' });
  }

  const scheduledDate = new Date(task.scheduledTime);
  const offsetMs = scheduledDate.getTimezoneOffset() * 60000;
  const localDate = new Date(scheduledDate.getTime() + offsetMs);
  const localIsoString = localDate.toISOString().slice(0, 16);

  res.json({
    success: true,
    message: task.message,
    scheduledTime: localIsoString,
    recipients: task.recipients || 'all'
  });
});

// ==================== ADMIN LIMITS PANEL ====================
app.get('/admin-limits', async function(req, res) {
  const totalUsers = await User.countDocuments({});
  const payingUsers = await User.countDocuments({ isSubscribed: true, subscriptionEndDate: { $gt: new Date() } });

  const html = '<!DOCTYPE html>\n' +
    '<html lang="en">\n' +
    '<head>\n' +
    '  <meta charset="UTF-8">\n' +
    '  <meta name="viewport" content="width=device-width, initial-scale=1.0">\n' +
    '  <title>Server Admin Panel</title>\n' +
    '  <style>\n' +
    '    body { font-family: \'Segoe UI\', sans-serif; background: #121212; color: #e0e0e0; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }\n' +
    '    .container { background: #1e1e1e; padding: 40px; border-radius: 12px; box-shadow: 0 8px 32px rgba(0,0,0,0.6); width: 90%; max-width: 600px; }\n' +
    '    h1 { text-align: center; color: #ffd700; margin-bottom: 30px; }\n' +
    '    .stats { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 30px; }\n' +
    '    .stat-box { background: #2d2d2d; padding: 20px; border-radius: 10px; text-align: center; }\n' +
    '    .stat-number { font-size: 2.5em; font-weight: bold; color: #00ff41; margin: 10px 0; }\n' +
    '    .stat-label { font-size: 1.1em; color: #aaa; }\n' +
    '    .pool { text-align: center; margin: 15px 0; padding: 12px; background: #2d2d2d; border-radius: 8px; font-size: 0.95em; color: #8fd; }\n' +
    '    label { display: block; margin: 20px 0 8px; font-size: 1.1em; }\n' +
    '    input[type="number"], input[type="password"] { width: 100%; padding: 12px; background: #2d2d2d; border: none; border-radius: 6px; color: white; font-size: 1em; margin-bottom: 15px; box-sizing: border-box; }\n' +
    '    button { width: 100%; padding: 14px; background: #ffd700; color: black; font-weight: bold; border: none; border-radius: 6px; cursor: pointer; font-size: 1.1em; margin-top: 20px; }\n' +
    '    button:hover { background: #e6c200; }\n' +
    '    .current { text-align: center; margin: 25px 0; padding: 15px; background: #2d2d2d; border-radius: 8px; font-size: 1.1em; }\n' +
    '  </style>\n' +
    '</head>\n' +
    '<body>\n' +
    '  <div class="container">\n' +
    '    <h1>Server Admin Panel</h1>\n' +
    '    <div class="pool">Bot pool: 1 auth bot + ' + botPool.poolSize + ' broadcast bots</div>\n' +
    '    <div class="stats">\n' +
    '      <div class="stat-box">\n' +
    '        <div class="stat-number">' + totalUsers + '</div>\n' +
    '        <div class="stat-label">Total Users</div>\n' +
    '      </div>\n' +
    '      <div class="stat-box">\n' +
    '        <div class="stat-number">' + payingUsers + '</div>\n' +
    '        <div class="stat-label">Paying Users</div>\n' +
    '      </div>\n' +
    '    </div>\n' +
    '    <form method="POST">\n' +
    '      <label>Owner Password</label>\n' +
    '      <input type="password" name="password" required placeholder="Enter admin password">\n' +
    '      <label>Daily Broadcasts per User (Free)</label>\n' +
    '      <input type="number" name="daily_broadcast" min="1" value="' + adminSettingsCache.dailyBroadcastLimit + '" required>\n' +
    '      <label>Max Landing Pages per User (Free)</label>\n' +
    '      <input type="number" name="max_pages" min="1" value="' + adminSettingsCache.maxLandingPages + '" required>\n' +
    '      <label>Max Forms per User (Free)</label>\n' +
    '      <input type="number" name="max_forms" min="1" value="' + adminSettingsCache.maxForms + '" required>\n' +
    '      <div class="current">\n' +
    '        <strong>Current Free Tier Limits:</strong><br>\n' +
    '        Broadcasts/day: ' + adminSettingsCache.dailyBroadcastLimit + ' | Pages: ' + adminSettingsCache.maxLandingPages + ' | Forms: ' + adminSettingsCache.maxForms + '\n' +
    '      </div>\n' +
    '      <button type="submit">Update Limits</button>\n' +
    '    </form>\n' +
    '  </div>\n' +
    '</body>\n' +
    '</html>';
  res.send(html);
});

app.post('/admin-limits', async function(req, res) {
  const { password, daily_broadcast, max_pages, max_forms } = req.body;

  if (password !== ADMIN_PASSWORD) {
    return res.send('<html><body style="background:#121212;color:#f44336;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;font-family:sans-serif;text-align:center;"><h1>Access Denied<br>Wrong Password</h1></body></html>');
  }

  const newDaily = parseInt(daily_broadcast);
  const newPages = parseInt(max_pages);
  const newForms = parseInt(max_forms);

  if (isNaN(newDaily) || isNaN(newPages) || isNaN(newForms) || newDaily < 1 || newPages < 1 || newForms < 1) {
    return res.send('<html><body style="background:#121212;color:#f44336;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;font-family:sans-serif;text-align:center;"><h1>Invalid Values<br>All limits must be ≥ 1</h1></body></html>');
  }

  try {
    await AdminSettings.updateSettings({
      dailyBroadcastLimit: newDaily,
      maxLandingPages: newPages,
      maxForms: newForms
    });

    adminSettingsCache = { dailyBroadcastLimit: newDaily, maxLandingPages: newPages, maxForms: newForms };

    console.log('Admin limits updated and saved to DB:', adminSettingsCache);

    res.send('<!DOCTYPE html>\n' +
      '<html lang="en">\n' +
      '<head>\n' +
      '  <meta charset="UTF-8">\n' +
      '  <title>Limits Updated</title>\n' +
      '  <style>\n' +
      '    body { font-family: \'Segoe UI\', sans-serif; background: #121212; color: #e0e0e0; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }\n' +
      '    .container { background: #1e1e1e; padding: 40px; border-radius: 12px; text-align: center; }\n' +
      '    h1 { color: #4caf50; }\n' +
      '    .success { font-size: 1.2em; margin: 20px 0; }\n' +
      '    a { color: #ffd700; text-decoration: none; font-weight: bold; }\n' +
      '    a:hover { text-decoration: underline; }\n' +
      '  </style>\n' +
      '</head>\n' +
      '<body>\n' +
      '  <div class="container">\n' +
      '    <h1>Success!</h1>\n' +
      '    <p class="success">Server limits updated and <strong>saved permanently</strong>:</p>\n' +
      '    <p><strong>Daily Broadcasts:</strong> ' + newDaily + '<br>\n' +
      '       <strong>Max Pages:</strong> ' + newPages + '<br>\n' +
      '       <strong>Max Forms:</strong> ' + newForms + '</p>\n' +
      '    <p><a href="/admin-limits">← Back to Control Panel</a></p>\n' +
      '  </div>\n' +
      '</body>\n' +
      '</html>');
  } catch (err) {
    console.error('Failed to save admin settings:', err);
    res.status(500).send('Failed to save settings');
  }
});

// ==================== STARTUP ====================
async function loadAdminSettings() {
  try {
    const settings = await AdminSettings.getSettings();
    adminSettingsCache = {
      dailyBroadcastLimit: settings.dailyBroadcastLimit,
      maxLandingPages: settings.maxLandingPages,
      maxForms: settings.maxForms
    };
    console.log('✅ Admin settings loaded from DB:', adminSettingsCache);
  } catch (err) {
    console.error('Failed to load admin settings:', err);
  }
}

mongoose.connection.once('open', async function() {
  try {
    await loadAdminSettings();

    // Bot pool: init once, register handlers once, set webhooks once.
    // No more per-user bot lifecycle — this happens exactly once at boot
    // regardless of how many Sendm users/contacts exist.
    await initBotPool();
    registerAuthBotHandlers(botPool.authBot);
    botPool.broadcastBots.forEach(function(entry) {
      registerBroadcastBotHandlers(entry.bot, entry.index);
    });
    await setupAllWebhooks();

    await recoverLostScheduledBroadcasts();

    serverReady = true;
    console.log('Startup sequence completed — server is now accepting bot-dependent requests');
  } catch (err) {
    // Do NOT let the process limp along with an empty bot pool — that's
    // what caused subscribes to fail with confusing Mongoose/undefined
    // errors while the server looked "up" from the outside. Fail loud so
    // Render restarts/reports the deploy as broken instead of routing
    // real traffic to a half-initialized instance.
    console.error('FATAL: startup sequence failed, exiting:', err.message);
    process.exit(1);
  }
});

process.on('SIGTERM', async function() {
  console.log('Shutting down gracefully...');
  await worker.close();
  await broadcastQueue.close();
  process.exit(0);
});

process.on('SIGINT', async function() {
  console.log('Shutting down gracefully...');
  await worker.close();
  await broadcastQueue.close();
  process.exit(0);
});

app.get('/ping', function(req, res) {
  if (!serverReady) return res.status(503).type('text/plain').send('starting up [' + BUILD_TAG + ']');
  res.status(200).type('text/plain').send('ok [' + BUILD_TAG + ']');
});

app.use(function(req, res) {
  res.status(404).render('404');
});

app.listen(PORT, function() {
  console.log('\nSENDEM SERVER — SHARED BOT-POOL MODEL (auth bot + broadcast pool)');
  console.log('Server running on port ' + PORT + ' | Domain: https://' + DOMAIN + '\n');
});
