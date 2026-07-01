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

const app = express();
const PORT = process.env.PORT || 3000;
app.set('trust proxy', 3);

// ==================== CONFIG & SECRETS ====================
const JWT_SECRET = process.env.JWT_SECRET || 'fallback_weak_secret_change_me_immediately';
const PAYSTACK_SECRET_KEY = process.env.PAYSTACK_SECRET_KEY || 'sk_test_fallback_change_me';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'midas';
const DOMAIN = process.env.DOMAIN;
let WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;

if (!DOMAIN) {
  console.error('ERROR: DOMAIN environment variable is required for webhooks!');
  process.exit(1);
}

if (!WEBHOOK_SECRET || WEBHOOK_SECRET.trim() === '') {
  WEBHOOK_SECRET = crypto.randomBytes(32).toString('hex');
  console.warn('⚠️  WARNING: WEBHOOK_SECRET not set in .env! Generated temporary one:');
  console.warn('     ' + WEBHOOK_SECRET);
  console.warn('     Add it to your .env file to keep it permanent across restarts:');
  console.warn('     WEBHOOK_SECRET=' + WEBHOOK_SECRET + '\n');
} else {
  console.log('Webhook secret loaded from .env');
}

if (JWT_SECRET.includes('fallback')) {
  console.warn('⚠️  WARNING: JWT_SECRET not set in .env! Using insecure fallback.');
}
if (PAYSTACK_SECRET_KEY.startsWith('sk_test_fallback')) {
  console.warn('⚠️  WARNING: PAYSTACK_SECRET_KEY not set in .env!');
}

const MONTHLY_PRICE_KOBO = 150000; // ₦5,000 in kobo

// Batching config
const BATCH_SIZE = 25;
const BATCH_INTERVAL_MS = 8000;
const MAX_MSG_LENGTH = 4000;

// Redis + BullMQ setup
let redisConnection;

if (process.env.REDIS_URL) {
  redisConnection = new IORedis(process.env.REDIS_URL, {
    maxRetriesPerRequest: null,
    enableReadyCheck: false
  });
} else {
  console.warn('⚠️ WARNING: REDIS_URL not set in .env, falling back to localhost:6379');
  redisConnection = new IORedis({
    host: 'localhost',
    port: 6379,
    maxRetriesPerRequest: null,
    enableReadyCheck: false
  });
}

const broadcastQueue = new Queue('telegram-broadcasts', { connection: redisConnection });

// ==================== CONTACT VALIDATION REGEX ====================
const CONTACT_REGEX = /^(\+?[0-9\s\-\(\)]{7,20}|[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$/;

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
    bucket = {
      pages: null,
      forms: null,
      contacts: null,
      pagesTs: 0,
      formsTs: 0,
      contactsTs: 0,
      lastAccess: Date.now()
    };
    userCache.set(userId, bucket);
  } else {
    bucket.lastAccess = Date.now();
  }
  return bucket;
}

function invalidateUserCache(userId, type = 'all') {
  const bucket = userCache.get(userId);
  if (!bucket) return;

  if (type === 'pages' || type === 'all') {
    bucket.pages = null;
    bucket.pagesTs = 0;
  }
  if (type === 'forms' || type === 'all') {
    bucket.forms = null;
    bucket.formsTs = 0;
  }
  if (type === 'contacts' || type === 'all') {
    bucket.contacts = null;
    bucket.contactsTs = 0;
  }
  bucket.lastAccess = Date.now();
}

function invalidatePublicCache(key) {
  publicCache.delete(key);
}

setInterval(() => {
  const now = Date.now();
  const INACTIVE_THRESHOLD = 30 * 60 * 1000;

  for (const [key, val] of publicCache.entries()) {
    if (now - val.timestamp > TTL.public) {
      publicCache.delete(key);
    }
  }

  for (const [userId, bucket] of userCache.entries()) {
    if (now - bucket.lastAccess > INACTIVE_THRESHOLD) {
      userCache.delete(userId);
      console.log('🧹 Cleaned cache for inactive user: ' + userId);
    }
  }
}, 10 * 60 * 1000);

// ==================== ADMIN SETTINGS CACHE ====================
let adminSettingsCache = {
  dailyBroadcastLimit: 3,
  maxLandingPages: 5,
  maxForms: 5
};

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
  telegramBotToken: String,
  telegramChatId: String,
  isTelegramConnected: { type: Boolean, default: false },
  botUsername: String,
  isSubscribed: { type: Boolean, default: false },
  subscriptionEndDate: Date,
  subscriptionPlan: String,
  pendingPaymentReference: String,
  createdAt: { type: Date, default: Date.now },
}, { timestamps: true });

userSchema.index({ telegramBotToken: 1 });

const landingPageSchema = new mongoose.Schema({
  shortId: { type: String, required: true, unique: true },
  userId: { type: String, required: true },
  title: String,
  config: Object,
  createdAt: Date,
  updatedAt: Date,
}, { timestamps: true });

const formPageSchema = new mongoose.Schema({
  shortId: { type: String, required: true, unique: true },
  userId: { type: String, required: true },
  title: String,
  state: Object,
  welcomeMessage: String,
  createdAt: Date,
  updatedAt: Date,
}, { timestamps: true });

const contactSchema = new mongoose.Schema({
  userId: { type: String, required: true },
  shortId: String,
  name: String,
  contact: { type: String, required: true },
  telegramChatId: String,
  status: { type: String, default: 'pending' },
  submittedAt: Date,
  subscribedAt: Date,
  unsubscribedAt: Date,
}, { timestamps: true });

const scheduledBroadcastSchema = new mongoose.Schema({
  broadcastId: { type: String, required: true, unique: true },
  userId: { type: String, required: true },
  message: String,
  recipients: { type: String, default: 'all' },
  scheduledTime: Date,
  status: { type: String, default: 'pending' },
  createdAt: Date,
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
  if (!settings) {
    settings = await this.create({
      dailyBroadcastLimit: 3,
      maxLandingPages: 5,
      maxForms: 5
    });
  }
  return settings;
};

adminSettingsSchema.statics.updateSettings = async function(updates) {
  let settings = await this.findOne();
  if (!settings) settings = new this();
  Object.assign(settings, updates);
  await settings.save();
  return settings;
};

// NEW: Persistent pending subscription store.
// This is the actual fix for the "Welcome! Subscribe from the form page..."
// bug: the old code only tracked pending subscriptions in an in-memory Map,
// which is wiped on every restart/redeploy. If a user submitted the form,
// then the server restarted before they tapped the Telegram deep link,
// the payload became unrecognized and fell through to the generic welcome
// message. Persisting to Mongo with a TTL index means it survives restarts.
const pendingSubscriptionSchema = new mongoose.Schema({
  payload: { type: String, required: true, unique: true },
  userId: { type: String, required: true },
  shortId: { type: String, required: true },
  name: { type: String, required: true },
  contact: { type: String, required: true },
  createdAt: { type: Date, default: Date.now, expires: 1800 } // auto-expire after 30 min
});

const AdminSettings = mongoose.model('AdminSettings', adminSettingsSchema);
const User = mongoose.model('User', userSchema);
const LandingPage = mongoose.model('LandingPage', landingPageSchema);
const FormPage = mongoose.model('FormPage', formPageSchema);
const Contact = mongoose.model('Contact', contactSchema);
const ScheduledBroadcast = mongoose.model('ScheduledBroadcast', scheduledBroadcastSchema);
const BroadcastDaily = mongoose.model('BroadcastDaily', broadcastDailySchema);
const PendingSubscription = mongoose.model('PendingSubscription', pendingSubscriptionSchema);

// Indexes
landingPageSchema.index({ userId: 1 });
formPageSchema.index({ userId: 1 });
contactSchema.index({ userId: 1 });
contactSchema.index({ userId: 1, contact: 1 });
contactSchema.index({ userId: 1, telegramChatId: 1 });
contactSchema.index({ userId: 1, status: 1 });
scheduledBroadcastSchema.index({ userId: 1 });
scheduledBroadcastSchema.index({ status: 1 });
scheduledBroadcastSchema.index({ scheduledTime: 1 });
broadcastDailySchema.index({ userId: 1, date: 1 }, { unique: true });

// In-memory fast-path cache for pending subscriptions (backed by Mongo above).
const pendingSubscribers = new Map();
const resetTokens = new Map();

// ==================== BOT MANAGER ====================
// Replaces the old ad-hoc activeBots Map + launchUserBot() free function.
//
// Design:
//   MongoDB (source of truth for token/username)
//        │
//        ▼
//   BotManager
//        │
//        ├── Cache (TTL) — keyed by userId, holds { bot, token, generation, webhookReadyAt }
//        │
//        └── Creates a Telegraf instance ON DEMAND (lazy), instead of eagerly
//            pre-launching every bot and hoping the cache never goes stale.
//               │
//               ▼
//            Webhook request comes in
//               │
//               ▼
//            BotManager.getBot(userId) — always resolves the CURRENT instance,
//            recreating it if the token changed or the cache entry expired.
//               │
//               ▼
//            bot.handleUpdate(update)
//
// This directly fixes the "stale undeleted bot Telegraf instance" bug:
// every mutation (connect / change-token / disconnect) bumps a per-user
// generation counter. Any in-flight async webhook-setup from an OLDER
// generation checks the generation before writing into the cache, so a
// slow/stale setup can never clobber a newer instance. Previously, the
// last async IIFE to finish won regardless of which call was actually
// more recent, which is exactly how a stale bot could stay wired to
// live traffic after a token change or reconnect.
class BotManager {
  constructor() {
    // userId -> { bot, token, generation, createdAt, webhookOk }
    this.cache = new Map();
    // userId -> generation counter, bumped on every launch/disconnect
    this.generations = new Map();
    this.CACHE_TTL_MS = 60 * 60 * 1000; // re-validate webhook at most hourly per user
  }

  _nextGeneration(userId) {
    const gen = (this.generations.get(userId) || 0) + 1;
    this.generations.set(userId, gen);
    return gen;
  }

  _currentGeneration(userId) {
    return this.generations.get(userId) || 0;
  }

  // Invalidate and drop any cached instance for a user (disconnect, token change).
  invalidate(userId) {
    this._nextGeneration(userId); // any in-flight setup for old gen is now stale
    this.cache.delete(userId);
  }

  // Returns a ready-to-use Telegraf instance for a user, creating one on
  // demand if missing, expired, or if the stored token no longer matches
  // the user's current token in Mongo.
  async getBot(userId) {
    const entry = this.cache.get(userId);

    if (entry && Date.now() - entry.createdAt < this.CACHE_TTL_MS) {
      return entry.bot;
    }

    // Cache miss or expired — (re)create on demand.
    const user = await User.findOne({ id: userId });
    if (!user || !user.telegramBotToken) {
      this.cache.delete(userId);
      return null;
    }

    // If we still have a live cached bot with the SAME token, just keep
    // using it and refresh its timestamp instead of tearing it down —
    // no need to recreate a Telegraf instance if nothing changed.
    if (entry && entry.token === user.telegramBotToken) {
      entry.createdAt = Date.now();
      return entry.bot;
    }

    return this._createBot(user);
  }

  // Force-create (or recreate) a bot instance for a user and register its
  // webhook. Called explicitly on connect / token change, and lazily from
  // getBot() on cache miss.
  async _createBot(user) {
    const generation = this._nextGeneration(user.id);
    const bot = new Telegraf(user.telegramBotToken);

    bot.webhookReply = false;
    bot.options.webhookReply = false;

    bot.catch((err) => {
      if (err.message && err.message.includes('Bot is not running')) {
        console.warn('Ignored expected "Bot is not running" warning in webhook mode for ' + user.email);
      } else {
        console.error('Bot error for ' + user.email + ':', err);
      }
    });

    this._registerHandlers(bot, user);

    // Register immediately in cache so concurrent webhook requests during
    // setup still get a usable (if not-yet-webhook-verified) instance.
    // handleUpdate() works regardless of whether setWebhook has finished;
    // it just processes the update object we hand it.
    if (this._currentGeneration(user.id) === generation) {
      this.cache.set(user.id, {
        bot,
        token: user.telegramBotToken,
        generation,
        createdAt: Date.now(),
        webhookOk: false
      });
    }

    // Fire-and-forget webhook (re)registration. Guarded by generation so a
    // slow/retrying setup from a superseded call can never overwrite a
    // newer bot instance in the cache.
    this._ensureWebhook(bot, user, generation).catch((err) => {
      console.error('Webhook setup failed permanently for ' + user.email + ':', err.message);
    });

    return bot;
  }

  _registerHandlers(bot, user) {
    bot.start(async (ctx) => {
      const payload = ctx.startPayload || '';
      const chatId = ctx.chat.id.toString();

      // DIAGNOSTIC: confirms which user's bot handler actually fired, and
      // the raw payload Telegram sent. Compare "handler.userId" against
      // "handler.botUsername" and the chat that triggered it.
      console.log('[SUB-DEBUG] /start received — handler.userId=' + user.id + ' handler.email=' + user.email + ' handler.botUsername=' + user.botUsername + ' payload="' + payload + '" chatId=' + chatId);

      if (payload.startsWith('sub_')) {
        const sub = await resolvePendingSubscription(payload, user.id);

        if (sub) {
          let targetContact = await Contact.findOne({
            userId: user.id,
            telegramChatId: chatId
          });

          const contactsByEmail = await Contact.find({ userId: user.id, contact: sub.contact });

          if (!targetContact) {
            targetContact = contactsByEmail.find(c => c.status === 'subscribed') ||
                            contactsByEmail.find(c => c.shortId === sub.shortId) ||
                            contactsByEmail[0];
          }

          if (!targetContact) {
            targetContact = new Contact({
              userId: user.id,
              shortId: sub.shortId,
              name: sub.name,
              contact: sub.contact,
              telegramChatId: chatId,
              status: 'subscribed',
              submittedAt: new Date(),
              subscribedAt: new Date()
            });
          } else {
            targetContact.name = sub.name;
            targetContact.contact = sub.contact;
            targetContact.shortId = sub.shortId;
            targetContact.telegramChatId = chatId;
            targetContact.status = 'subscribed';
            targetContact.subscribedAt = targetContact.subscribedAt || new Date();
            targetContact.submittedAt = new Date();
          }

          await targetContact.save();

          await Contact.deleteMany({
            userId: user.id,
            $or: [
              { contact: sub.contact, _id: { $ne: targetContact._id } },
              { telegramChatId: chatId, _id: { $ne: targetContact._id } }
            ]
          });

          await clearPendingSubscription(payload);

          const form = await FormPage.findOne({ shortId: sub.shortId });
          let welcomeText = '<b>Subscription Confirmed!</b>\n\nHi <b>' + escapeHtml(sub.name) + '</b>!\n\nYou\'re now subscribed.\n\nThank you';

          if (form && form.welcomeMessage && form.welcomeMessage.trim()) {
            welcomeText = form.welcomeMessage
              .replace(/\{name\}/gi, '<b>' + escapeHtml(sub.name) + '</b>')
              .replace(/\{contact\}/gi, escapeHtml(sub.contact));
          }

          await ctx.replyWithHTML(welcomeText);
          return;
        }

        // Payload looked like a subscription attempt but we couldn't resolve
        // it (truly expired/consumed) — tell the user plainly instead of
        // silently showing the generic welcome message, which was the
        // confusing part of the original bug.
        await ctx.replyWithHTML('<b>This subscription link has expired.</b>\n\nPlease go back to the form and submit it again.');
        return;
      }

      if (payload === user.id) {
        const freshUser = await User.findOne({ id: user.id });
        if (freshUser) {
          freshUser.telegramChatId = chatId;
          freshUser.isTelegramConnected = true;
          await freshUser.save();
        }
        await ctx.replyWithHTML('<b>Sendm 2FA Connected Successfully!</b>\n\nYou will receive login codes here.');
        return;
      }

      await ctx.replyWithHTML('<b>Welcome!</b>\n\nSubscribe from the page to get updates.');
    });

    bot.command('status', async (ctx) => {
      const freshUser = await User.findOne({ id: user.id });
      const connected = freshUser ? freshUser.isTelegramConnected : false;
      await ctx.replyWithHTML('<b>Sendm 2FA Status</b>\nAccount: <code>' + user.email + '</code>\nStatus: <b>' + (connected ? 'Connected' : 'Not Connected') + '</b>');
    });
  }

  async _ensureWebhook(bot, user, generation) {
    const webhookPath = '/webhook/' + WEBHOOK_SECRET + '/' + user.id;
    const webhookUrl = 'https://' + DOMAIN + webhookPath;

    const isStale = () => this._currentGeneration(user.id) !== generation;

    try {
      const current = await bot.telegram.getWebhookInfo();
      if (isStale()) return;

      const alreadyCorrect =
        current.url === webhookUrl &&
        !current.has_custom_certificate &&
        current.pending_update_count < 50;

      if (alreadyCorrect) {
        console.log('Webhook already correct for ' + user.email);
        this._markWebhookOk(user.id, generation);
        return;
      }

      console.log('Webhook needs update for ' + user.email + ' → current: ' + (current.url || 'none'));

      await bot.telegram.deleteWebhook({ drop_pending_updates: true });
      if (isStale()) return;
      console.log('Webhook cleaned for ' + user.email);

      await new Promise(resolve => setTimeout(resolve, 2500));
      if (isStale()) return;

      let attempts = 0;
      const maxAttempts = 5;

      while (attempts < maxAttempts) {
        if (isStale()) return;
        try {
          const success = await bot.telegram.setWebhook(webhookUrl, {
            allowed_updates: ['message', 'callback_query', 'my_chat_member']
          });

          if (success) {
            console.log('Webhook SUCCESSFULLY set for @' + (user.botUsername || 'unknown') + ' → ' + webhookUrl);
            this._markWebhookOk(user.id, generation);
            return;
          }
        } catch (err) {
          attempts++;
          if (err.response && err.response.error_code === 429) {
            const retryAfter = err.response.parameters?.retry_after || 30;
            console.warn('Rate limit hit for ' + user.email + ' - waiting ' + (retryAfter + 5) + 's (attempt ' + attempts + '/' + maxAttempts + ')');
            await new Promise(r => setTimeout(r, (retryAfter + 5) * 1000));
          } else {
            console.error('Webhook set FAILED for ' + user.email + ': ' + err.message);
            if (attempts >= maxAttempts) throw err;
            await new Promise(r => setTimeout(r, 5000));
          }
        }
      }

      console.error('Gave up setting webhook for ' + user.email + ' after ' + maxAttempts + ' attempts');
    } catch (err) {
      console.error('Webhook setup completely failed for ' + user.email + ': ' + err.message);
    }
  }

  _markWebhookOk(userId, generation) {
    const entry = this.cache.get(userId);
    if (entry && entry.generation === generation) {
      entry.webhookOk = true;
      entry.createdAt = Date.now();
    }
  }

  // Explicitly launch/relaunch a user's bot (connect, token change, startup).
  // This ALWAYS creates a fresh instance and registers it, bypassing the
  // "same token, keep old instance" shortcut in getBot() — used when we
  // know the token or connection state has genuinely changed.
  async launch(user) {
    this.invalidate(user.id); // bump generation so any old in-flight setup is discarded
    if (!user.telegramBotToken) return null;
    return this._createBot(user);
  }

  // Send a message using a user's current bot, resolving it on demand.
  async sendMessage(userId, chatId, text, extra) {
    const bot = await this.getBot(userId);
    if (!bot) throw new Error('Telegram bot not connected');
    return bot.telegram.sendMessage(chatId, text, extra);
  }
}

const botManager = new BotManager();

// ==================== PENDING SUBSCRIPTION HELPERS ====================
// Mongo-backed with an in-memory fast path. This is what actually fixes
// the "shows default Welcome message" bug: even if the process restarts
// between form-submit and the user tapping the Telegram deep link, the
// pending subscription is still recoverable from Mongo.
async function createPendingSubscription(payload, userId, shortId, name, contact) {
  const doc = { payload, userId, shortId, name, contact, createdAt: new Date() };
  pendingSubscribers.set(payload, doc);
  await PendingSubscription.findOneAndUpdate(
    { payload },
    doc,
    { upsert: true }
  );
}

async function resolvePendingSubscription(payload, userId) {
  const cached = pendingSubscribers.get(payload);
  if (cached && cached.userId === userId) {
    console.log('[SUB-DEBUG] resolved from memory cache: payload=' + payload + ' userId=' + userId);
    return cached;
  }

  // DIAGNOSTIC: log why the in-memory cache missed, and check whether the
  // doc exists at all in Mongo (regardless of userId) vs. truly not existing.
  if (cached) {
    console.log('[SUB-DEBUG] memory cache HIT but userId mismatch: payload=' + payload + ' cached.userId=' + cached.userId + ' requested.userId=' + userId);
  } else {
    console.log('[SUB-DEBUG] memory cache MISS: payload=' + payload + ' userId=' + userId);
  }

  const doc = await PendingSubscription.findOne({ payload, userId }).lean();
  if (doc) {
    console.log('[SUB-DEBUG] resolved from Mongo: payload=' + payload + ' userId=' + userId);
    pendingSubscribers.set(payload, doc);
    return doc;
  }

  // Check if the doc exists under a DIFFERENT userId — this tells us if the
  // payload was created for one user but looked up under another.
  const anyDoc = await PendingSubscription.findOne({ payload }).lean();
  if (anyDoc) {
    console.log('[SUB-DEBUG] payload exists in Mongo but for a DIFFERENT userId! payload=' + payload + ' doc.userId=' + anyDoc.userId + ' requested.userId=' + userId);
  } else {
    console.log('[SUB-DEBUG] payload does not exist in Mongo at all (expired/never created/already consumed): payload=' + payload);
  }

  return null;
}

async function clearPendingSubscription(payload) {
  pendingSubscribers.delete(payload);
  await PendingSubscription.deleteOne({ payload }).catch(() => {});
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
  keyGenerator: function(req) {
    return req.ip + '::' + req.params.shortId;
  },
  skip: function(req) {
    return !req.params.shortId;
  }
});

// ==================== WEBHOOK ENDPOINT ====================
// Resolves the bot via BotManager.getBot() ON DEMAND instead of reading a
// possibly-stale/possibly-missing entry from a plain Map. This closes the
// original race: if the cache is empty (fresh restart) or expired, getBot()
// transparently creates the instance right here before handling the update,
// instead of silently dropping the update.
app.post('/webhook/' + WEBHOOK_SECRET + '/:userId', async (req, res) => {
  const userId = req.params.userId;

  let update;
  try {
    if (Buffer.isBuffer(req.body)) {
      update = JSON.parse(req.body.toString('utf8'));
    } else if (req.body && typeof req.body === 'object') {
      update = req.body;
    } else {
      throw new Error('Invalid body format');
    }
  } catch (err) {
    console.error('Failed to parse webhook body for user ' + userId + ':', err);
    return res.sendStatus(400);
  }

  // Acknowledge Telegram immediately so it never retries/duplicates due to
  // slow handling on our end, then process the update.
  res.sendStatus(200);

  try {
    const bot = await botManager.getBot(userId);
    if (bot) {
      await bot.handleUpdate(update);
    } else {
      console.warn('No bot instance available for user ' + userId + ' (no token on file)');
    }
  } catch (err) {
    console.error('Webhook handle error for user ' + userId + ':', err);
  }
});

// ==================== UTILITIES ====================
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

function escapeHtml(unsafe) {
  if (!unsafe) unsafe = '';
  return unsafe.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}

function textToHtmlForDisplay(text) {
  if (!text) return '';

  return text
    .replace(/\n{2,}/g, '</p><p>')
    .replace(/\n/g, '<br>');
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
      if (current) {
        chunks.push(current.trim());
        current = '';
      }
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
  if (hasActiveSubscription(user)) {
    return { dailyBroadcasts: Infinity, maxLandingPages: Infinity, maxForms: Infinity };
  }
  return {
    dailyBroadcasts: adminSettingsCache.dailyBroadcastLimit,
    maxLandingPages: adminSettingsCache.maxLandingPages,
    maxForms: adminSettingsCache.maxForms
  };
}

async function incrementDailyBroadcast(userId) {
  const today = getTodayDateString();
  const record = await BroadcastDaily.findOneAndUpdate(
    { userId: userId, date: today },
    { $inc: { count: 1 } },
    { upsert: true, new: true }
  );
  return record.count;
}

// ==================== BullMQ Worker ====================
async function processBroadcast(job) {
  const { userId, message, broadcastId } = job.data;

  const bot = await botManager.getBot(userId);
  if (!bot) {
    throw new Error('Telegram bot not connected');
  }

  const chunks = splitTelegramMessage(message);

  const targets = await Contact.find({
    userId: userId,
    status: 'subscribed',
    telegramChatId: { $exists: true, $ne: null }
  });

  const total = targets.length;
  let sent = 0;
  let failed = 0;

  const batches = [];
  for (let i = 0; i < targets.length; i += BATCH_SIZE) {
    batches.push(targets.slice(i, i + BATCH_SIZE));
  }

  for (let b = 0; b < batches.length; b++) {
    const batch = batches[b];

    const sendPromises = batch.map(async function(target) {
      try {
        for (const chunk of chunks) {
          await bot.telegram.sendMessage(target.telegramChatId, chunk, { parse_mode: 'HTML' });
        }
        sent++;
      } catch (err) {
        failed++;
        const isBlocked = err.response?.error_code === 403 ||
          /blocked|forbidden|chat not found|deactivated/i.test(err.message || '');
        if (isBlocked) {
          await Contact.findByIdAndUpdate(target._id, {
            status: 'unsubscribed',
            unsubscribedAt: new Date(),
            telegramChatId: null
          });
        }
      }
    });

    await Promise.all(sendPromises);

    if (b < batches.length - 1) {
      await new Promise(resolve => setTimeout(resolve, BATCH_INTERVAL_MS));
    }
  }

  const user = await User.findOne({ id: userId });
  let reportText = broadcastId ? '<b>Scheduled Broadcast Report</b>\n\n' : '<b>Broadcast Report</b>\n\n';
  if (total === 0) {
    reportText += 'No subscribed contacts with Telegram connected.';
  } else {
    const emoji = failed === 0 ? '✅' : '⚠️';
    reportText += '(' + emoji + ' <b>' + sent + ' of ' + total + '</b> delivered.\n';
    if (failed > 0) reportText += failed + ' failed.';
  }
  reportText += '\n\nTime: ' + new Date().toLocaleString();

  if (user && user.isTelegramConnected && user.telegramChatId) {
    try {
      await bot.telegram.sendMessage(user.telegramChatId, reportText, { parse_mode: 'HTML' });
    } catch (err) {
      console.error('Failed to send report to user ' + userId, err);
    }
  }

  if (broadcastId) {
    await ScheduledBroadcast.deleteOne({ broadcastId: broadcastId });
  }

  invalidateUserCache(userId, 'contacts');
}

const worker = new Worker('telegram-broadcasts', processBroadcast, {
  connection: redisConnection,
  concurrency: 4
});

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
    try {
      const bot = await botManager.getBot(userId);
      if (bot) {
        const text = broadcastId
          ? '<b>Scheduled Broadcast Failed</b>\n\nFailed after retries.\nError: ' + err.message
          : '<b>Broadcast Failed</b>\n\nFailed after retries.\nError: ' + err.message;
        await bot.telegram.sendMessage(user.telegramChatId, text, { parse_mode: 'HTML' });
      }
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
    if (existing) {
      alreadyExists++;
      continue;
    }

    const delayMs = task.scheduledTime.getTime() - Date.now();

    if (delayMs <= 1000) {
      await broadcastQueue.add(
        'send-broadcast',
        {
          userId: task.userId,
          message: task.message,
          broadcastId: task.broadcastId
        },
        {
          jobId: task.broadcastId,
          attempts: 4,
          backoff: { type: 'exponential', delay: 5000 }
        }
      );
    } else {
      await broadcastQueue.add(
        'send-broadcast',
        {
          userId: task.userId,
          message: task.message,
          broadcastId: task.broadcastId
        },
        {
          jobId: task.broadcastId,
          delay: delayMs,
          attempts: 4,
          backoff: { type: 'exponential', delay: 5000 }
        }
      );
    }

    recovered++;
  }

  console.log(
    '✓ Recovery completed: ' + recovered + ' broadcast(s) re-queued, ' +
    alreadyExists + ' were already present in queue'
  );
}

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

app.post('/api/auth/connect-telegram', authenticateToken, async function(req, res) {
  const { botToken } = req.body;
  if (!botToken || !botToken.trim()) return res.status(400).json({ error: 'Bot token required' });

  const token = botToken.trim();

  const existingUser = await User.findOne({ telegramBotToken: token });
  if (existingUser && existingUser.id !== req.user.id) {
    return res.status(400).json({ error: 'This bot is already linked to another account.' });
  }

  // Retry validation for network issues
  let botInfo;
  let attempts = 0;
  const maxAttempts = 7;
  while (attempts < maxAttempts) {
    attempts++;
    try {
      const response = await axios.get('https://api.telegram.org/bot' + token + '/getMe', {
        timeout: 20000
      });
      if (!response.data.ok) {
        return res.status(400).json({
          error: 'Invalid bot token – Telegram rejected it: ' + (response.data.description || 'Unauthorized')
        });
      }
      botInfo = response.data.result;
      if (!botInfo || !botInfo.username) {
        return res.status(400).json({ error: 'Invalid response – missing bot username' });
      }
      break;
    } catch (err) {
      console.warn('Bot token validation attempt ' + attempts + '/' + maxAttempts + ' failed: ' + (err.message || err.code));
      if (attempts >= maxAttempts) {
        return res.status(500).json({ error: 'Network error validating bot token. Please try again later.' });
      }
      await new Promise(r => setTimeout(r, 8000));
    }
  }

  const botUsername = botInfo.username.replace(/^@/, '');

  // Clear old webhook if token is different
  if (req.user.telegramBotToken && req.user.telegramBotToken !== token) {
    try {
      await axios.post('https://api.telegram.org/bot' + req.user.telegramBotToken + '/deleteWebhook', {
        drop_pending_updates: true
      }, { timeout: 20000 });
      console.log('Old webhook cleared before connecting new bot for user ' + req.user.id);
    } catch (err) {
      console.warn('Failed to clear old webhook (may be invalid token): ' + err.message);
    }
  }

  req.user.telegramBotToken = token;
  req.user.botUsername = botUsername;
  req.user.isTelegramConnected = false;
  req.user.telegramChatId = null;
  await req.user.save();

  await botManager.launch(req.user);

  const startLink = 'https://t.me/' + botUsername + '?start=' + req.user.id;

  res.json({
    success: true,
    message: 'Bot connected!',
    botUsername: '@' + botUsername,
    startLink: startLink
  });
});

app.post('/api/auth/change-bot-token', authenticateToken, async function(req, res) {
  const { newBotToken } = req.body;
  if (!newBotToken || !newBotToken.trim()) return res.status(400).json({ error: 'New bot token required' });

  const token = newBotToken.trim();

  const existingUser = await User.findOne({ telegramBotToken: token });
  if (existingUser && existingUser.id !== req.user.id) {
    return res.status(400).json({ error: 'This bot is already linked to another account.' });
  }

  // Retry validation for network issues
  let botInfo;
  let attempts = 0;
  const maxAttempts = 7;
  while (attempts < maxAttempts) {
    attempts++;
    try {
      const response = await axios.get('https://api.telegram.org/bot' + token + '/getMe', {
        timeout: 20000
      });
      if (!response.data.ok) {
        return res.status(400).json({
          error: 'Invalid new token – Telegram rejected it: ' + (response.data.description || 'Unauthorized')
        });
      }
      botInfo = response.data.result;
      if (!botInfo || !botInfo.username) {
        return res.status(400).json({ error: 'Invalid response – missing bot username' });
      }
      break;
    } catch (err) {
      console.warn('New bot token validation attempt ' + attempts + '/' + maxAttempts + ' failed: ' + (err.message || err.code));
      if (attempts >= maxAttempts) {
        return res.status(500).json({ error: 'Network error validating new bot token. Please try again later.' });
      }
      await new Promise(r => setTimeout(r, 8000));
    }
  }

  const botUsername = botInfo.username.replace(/^@/, '');

  // Always clear old webhook on token change
  if (req.user.telegramBotToken) {
    try {
      await axios.post('https://api.telegram.org/bot' + req.user.telegramBotToken + '/deleteWebhook', {
        drop_pending_updates: true
      }, { timeout: 20000 });
      console.log('Old webhook cleared on bot token change for user ' + req.user.id);
    } catch (err) {
      console.warn('Failed to clear old webhook on token change: ' + err.message);
    }
  }

  req.user.telegramBotToken = token;
  req.user.botUsername = botUsername;
  req.user.isTelegramConnected = false;
  req.user.telegramChatId = null;
  await req.user.save();

  await botManager.launch(req.user);

  const startLink = 'https://t.me/' + botUsername + '?start=' + req.user.id;

  res.json({
    success: true,
    message: 'Bot token updated! Please send /start to the new bot to reconnect 2FA.',
    botUsername: '@' + botUsername,
    startLink: startLink
  });
});

app.post('/api/auth/disconnect-telegram', authenticateToken, async function(req, res) {
  // Clear webhook via direct API
  if (req.user.telegramBotToken) {
    try {
      await axios.post('https://api.telegram.org/bot' + req.user.telegramBotToken + '/deleteWebhook', {
        drop_pending_updates: true
      }, { timeout: 20000 });
      console.log('Webhook cleared on disconnect for user ' + req.user.id);
    } catch (err) {
      console.warn('Failed to clear webhook on disconnect: ' + err.message);
    }
  }

  // Fully invalidate the cached bot instance (bumps generation, drops cache entry)
  botManager.invalidate(req.user.id);

  req.user.telegramBotToken = null;
  req.user.botUsername = null;
  req.user.telegramChatId = null;
  req.user.isTelegramConnected = false;
  await req.user.save();

  res.json({ success: true, message: 'Telegram disconnected successfully. You can now connect a fresh bot without any blockage.' });
});

app.get('/api/auth/bot-status', authenticateToken, function(req, res) {
  res.json({
    activated: req.user.isTelegramConnected,
    chatId: req.user.telegramChatId || null
  });
});

function generate2FACode() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

async function send2FACodeViaBot(user, code) {
  if (!user.isTelegramConnected || !user.telegramChatId) return false;
  try {
    await botManager.sendMessage(
      user.id,
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

// ==================== SUBSCRIPTION ROUTES ====================
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
      {
        headers: {
          Authorization: 'Bearer ' + PAYSTACK_SECRET_KEY,
          'Content-Type': 'application/json'
        }
      }
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
      return {
        ...block,
        htmlContent: textToHtmlForDisplay(block.content)
      };
    }
    return block;
  });

  const data = {
    title: page.title,
    blocks: processedBlocks
  };

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

  const data = {
    shortId: form.shortId,
    title: form.title,
    state: form.state,
    welcomeMessage: form.welcomeMessage
  };
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
    {
      userId: req.user.id,
      title: title.trim(),
      config: { blocks: cleanBlocks },
      updatedAt: now,
      createdAt: shortId ? undefined : now
    },
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
    {
      userId: req.user.id,
      title: title.trim(),
      state: sanitizedState,
      welcomeMessage: sanitizedWelcome,
      updatedAt: now,
      createdAt: shortId ? undefined : now
    },
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
  if (!owner || !owner.telegramBotToken || !owner.botUsername) return res.status(400).json({ error: 'Bot not connected' });

  const payload = 'sub_' + shortId + '_' + uuidv4().slice(0, 12);

  let contact = await Contact.findOne({ userId: owner.id, contact: contactValue });
  let alreadySubscribed = false;

  if (contact) {
    if (contact.status === 'subscribed') {
      contact.name = name.trim();
      contact.shortId = shortId;
      contact.submittedAt = new Date();
      await contact.save();
      alreadySubscribed = true;
    } else {
      contact.name = name.trim();
      contact.shortId = shortId;
      contact.submittedAt = new Date();
      await contact.save();
    }
  } else {
    contact = new Contact({
      userId: owner.id,
      shortId: shortId,
      name: name.trim(),
      contact: contactValue,
      status: 'pending',
      submittedAt: new Date()
    });
    await contact.save();
  }

  // Persist to Mongo (source of truth) + in-memory fast path.
  await createPendingSubscription(payload, owner.id, shortId, name.trim(), contactValue);

  const deepLink = 'https://t.me/' + owner.botUsername + '?start=' + payload;
  res.json({ success: true, deepLink: deepLink, alreadySubscribed: alreadySubscribed });

  invalidateUserCache(owner.id, 'contacts');
});

app.post('/api/contacts/delete', authenticateToken, async function(req, res) {
  const { contacts } = req.body;
  if (!Array.isArray(contacts) || contacts.length === 0) return res.status(400).json({ error: 'Provide contact array' });

  const result = await Contact.deleteMany({
    userId: req.user.id,
    contact: { $in: contacts }
  });

  invalidateUserCache(req.user.id, 'contacts');

  res.json({ success: true, deletedCount: result.deletedCount });
});

// ==================== BROADCASTING ====================
app.post('/api/broadcast/now', authenticateToken, async function(req, res) {
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

  const broadcast = await ScheduledBroadcast.create({
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
  if (job) {
    await job.remove();
  }

  await task.deleteOne();

  res.json({ success: true });
});

app.patch('/api/broadcast/scheduled/:broadcastId', authenticateToken, async function(req, res) {
  const { message, scheduledTime, recipients } = req.body;
  const task = await ScheduledBroadcast.findOne({ broadcastId: req.params.broadcastId, userId: req.user.id, status: 'pending' });

  if (!task) return res.status(400).json({ error: 'Cannot edit this broadcast' });

  const oldJob = await broadcastQueue.getJob(task.broadcastId);
  if (oldJob) {
    await oldJob.remove();
  }

  let needsUpdate = false;

  if (message && message.trim()) {
    const processed = message.trim();
    if (processed.length > MAX_MSG_LENGTH * 10) {
      return res.status(400).json({ error: 'Message too long' });
    }
    const readyMessage = prepareTelegramMessage(processed);
    task.message = readyMessage;
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
    '    label { display: block; margin: 20px 0 8px; font-size: 1.1em; }\n' +
    '    input[type="number"], input[type="password"] { width: 100%; padding: 12px; background: #2d2d2d; border: none; border-radius: 6px; color: white; font-size: 1em; margin-bottom: 15px; }\n' +
    '    button { width: 100%; padding: 14px; background: #ffd700; color: black; font-weight: bold; border: none; border-radius: 6px; cursor: pointer; font-size: 1.1em; margin-top: 20px; }\n' +
    '    button:hover { background: #e6c200; }\n' +
    '    .current { text-align: center; margin: 25px 0; padding: 15px; background: #2d2d2d; border-radius: 8px; font-size: 1.1em; }\n' +
    '  </style>\n' +
    '</head>\n' +
    '<body>\n' +
    '  <div class="container">\n' +
    '    <h1>Server Admin Panel</h1>\n' +
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

    adminSettingsCache = {
      dailyBroadcastLimit: newDaily,
      maxLandingPages: newPages,
      maxForms: newForms
    };

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

// ==================== CLEANUP ====================
// In-memory pendingSubscribers fast-path cleanup. Mongo-side cleanup is
// handled automatically by the TTL index on PendingSubscription.createdAt.
setInterval(function() {
  const now = Date.now();
  const keys = Array.from(pendingSubscribers.keys());
  for (const key of keys) {
    const data = pendingSubscribers.get(key);
    const createdAt = data.createdAt instanceof Date ? data.createdAt.getTime() : data.createdAt;
    if (now - createdAt > 30 * 60 * 1000) {
      pendingSubscribers.delete(key);
    }
  }
}, 60 * 60 * 1000);

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
  await loadAdminSettings();

  // NOTE: bots are now created ON DEMAND by BotManager (see class above),
  // not eagerly pre-launched for every user at startup. This avoids the
  // startup thundering-herd of setWebhook calls hitting Telegram's rate
  // limits, and avoids ever serving a webhook request from a bot instance
  // that was created before we knew if the token was still valid.
  // The first webhook request (or /start, or a broadcast) for each user
  // will lazily create and cache their bot instance via botManager.getBot().
  console.log('Bots will be created on demand via BotManager (lazy, webhook-driven)');

  await recoverLostScheduledBroadcasts();

  console.log('Startup sequence completed');
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
  res.status(200).type('text/plain').send('ok');
});

app.use(function(req, res) {
  res.status(404).render('404');
});

app.listen(PORT, function() {
  console.log('\nSENDEM SERVER — BotManager (on-demand Telegraf instances) + BullMQ + Redis BROADCAST QUEUE');
  console.log('Server running on port ' + PORT + ' | Domain: https://' + DOMAIN + '\n');
});
