'use strict';

const { Telegraf }          = require('telegraf');
const axios                 = require('axios');
const { DOMAIN, WEBHOOK_SECRET } = require('./config');
const { escapeHtml }        = require('./utils');
const { Contact, FormPage, User } = require('./models');

// ==================== IN-MEMORY REGISTRIES ====================
/** @type {Map<string, import('telegraf').Telegraf>} */
const activeBots = new Map();

/** @type {Map<string, number>} userId → epoch ms of last webhook set */
const lastWebhookSetTime = new Map();

/** @type {Map<string, {userId, shortId, name, contact, createdAt}>} */
const pendingSubscribers = new Map();

// Cleanup pending subscribers hourly
const _cleanup = setInterval(() => {
  const cutoff = Date.now() - 30 * 60 * 1000;
  for (const [key, data] of pendingSubscribers.entries()) {
    if (data.createdAt < cutoff) pendingSubscribers.delete(key);
  }
}, 60 * 60 * 1000);
_cleanup.unref();

// ==================== LAUNCH ONE BOT ====================
function launchUserBot(user) {
  if (!user.telegramBotToken) return;

  // Drop stale instance (pure webhook mode — no .stop() needed)
  if (activeBots.has(user.id)) activeBots.delete(user.id);

  const bot = new Telegraf(user.telegramBotToken);
  bot.webhookReply          = false;
  bot.options.webhookReply  = false;

  bot.catch((err) => {
    if (err?.message?.includes('Bot is not running')) return;
    console.error(`Bot error for ${user.email}:`, err.message);
  });

  // /start handler
  bot.start(async (ctx) => {
    const payload = ctx.startPayload || '';
    const chatId  = ctx.chat.id.toString();

    // --- Subscription flow ---
    if (payload.startsWith('sub_') && pendingSubscribers.has(payload)) {
      const sub = pendingSubscribers.get(payload);
      if (sub.userId === user.id) {
        await handleSubscription(ctx, user, sub, payload, chatId);
        return;
      }
    }

    // --- 2FA connect flow ---
    if (payload === user.id) {
      user.telegramChatId      = chatId;
      user.isTelegramConnected = true;
      await User.findOneAndUpdate({ id: user.id }, {
        telegramChatId: chatId,
        isTelegramConnected: true
      });
      await ctx.replyWithHTML('<b>Sendm 2FA Connected!</b>\n\nYou will receive login codes here.');
      return;
    }

    await ctx.replyWithHTML('<b>Welcome!</b>\n\nSubscribe from the form page to receive updates.');
  });

  // /status command
  bot.command('status', async (ctx) => {
    await ctx.replyWithHTML(
      `<b>Sendm Status</b>\n` +
      `Account: <code>${escapeHtml(user.email)}</code>\n` +
      `Telegram: <b>${user.isTelegramConnected ? '✅ Connected' : '❌ Not connected'}</b>`
    );
  });

  // Register bot instance immediately so it can receive updates
  activeBots.set(user.id, bot);

  // Set webhook asynchronously
  _setupWebhook(bot, user).catch((err) => {
    console.error(`Webhook setup failed for ${user.email}:`, err.message);
  });
}

async function handleSubscription(ctx, user, sub, payload, chatId) {
  try {
    // Find or create contact
    let target = await Contact.findOne({ userId: user.id, telegramChatId: chatId });

    if (!target) {
      const byContact = await Contact.find({ userId: user.id, contact: sub.contact });
      target = byContact.find(c => c.status === 'subscribed')
             || byContact.find(c => c.shortId === sub.shortId)
             || byContact[0]
             || null;
    }

    if (!target) {
      target = new Contact({
        userId:         user.id,
        shortId:        sub.shortId,
        name:           sub.name,
        contact:        sub.contact,
        telegramChatId: chatId,
        status:         'subscribed',
        submittedAt:    new Date(),
        subscribedAt:   new Date()
      });
    } else {
      target.name           = sub.name;
      target.contact        = sub.contact;
      target.shortId        = sub.shortId;
      target.telegramChatId = chatId;
      target.status         = 'subscribed';
      target.subscribedAt   = target.subscribedAt || new Date();
      target.submittedAt    = new Date();
    }

    await target.save();

    // Remove duplicate contacts for same email/phone or chatId
    await Contact.deleteMany({
      userId: user.id,
      $or: [
        { contact: sub.contact,   _id: { $ne: target._id } },
        { telegramChatId: chatId, _id: { $ne: target._id } }
      ]
    });

    pendingSubscribers.delete(payload);

    // Build welcome message
    const form = await FormPage.findOne({ shortId: sub.shortId }).lean();
    let welcomeText = `<b>Subscription Confirmed!</b>\n\nHi <b>${escapeHtml(sub.name)}</b>!\n\nYou're now subscribed. Thank you.`;

    if (form?.welcomeMessage?.trim()) {
      welcomeText = form.welcomeMessage
        .replace(/\{name\}/gi,    `<b>${escapeHtml(sub.name)}</b>`)
        .replace(/\{contact\}/gi, escapeHtml(sub.contact));
    }

    await ctx.replyWithHTML(welcomeText);
  } catch (err) {
    console.error(`Subscription handler error for user ${user.id}:`, err.message);
    await ctx.replyWithHTML('<b>Something went wrong.</b> Please try again.');
  }
}

async function _setupWebhook(bot, user) {
  const webhookPath = `/webhook/${WEBHOOK_SECRET}/${user.id}`;
  const webhookUrl  = `https://${DOMAIN}${webhookPath}`;

  const WEBHOOK_REFRESH_MS = 30 * 60 * 1000;
  const MAX_ATTEMPTS = 5;

  try {
    const current = await bot.telegram.getWebhookInfo();
    const lastSet  = lastWebhookSetTime.get(user.id) || 0;
    const fresh    = Date.now() - lastSet < WEBHOOK_REFRESH_MS;

    if (current.url === webhookUrl && !current.has_custom_certificate && fresh) {
      console.log(`Webhook already current for ${user.email} — skipping`);
      return;
    }

    if (current.url === webhookUrl && !current.has_custom_certificate) {
      lastWebhookSetTime.set(user.id, Date.now());
      console.log(`Webhook URL correct for ${user.email} — timestamp refreshed`);
      return;
    }

    console.log(`Setting webhook for ${user.email}: ${webhookUrl}`);
    await bot.telegram.deleteWebhook({ drop_pending_updates: true });
    await _sleep(4000);

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        const ok = await bot.telegram.setWebhook(webhookUrl, {
          allowed_updates: ['message', 'callback_query', 'my_chat_member']
        });
        if (ok) {
          console.log(`✅ Webhook set for @${user.botUsername || 'unknown'}`);
          lastWebhookSetTime.set(user.id, Date.now());
          return;
        }
      } catch (err) {
        if (err?.response?.error_code === 429) {
          const wait = ((err.response.parameters?.retry_after || 30) + 5) * 1000;
          console.warn(`Rate-limited for ${user.email} — waiting ${wait / 1000}s (attempt ${attempt}/${MAX_ATTEMPTS})`);
          await _sleep(wait);
        } else {
          console.error(`Webhook attempt ${attempt} failed for ${user.email}: ${err.message}`);
          if (attempt >= MAX_ATTEMPTS) throw err;
          await _sleep(5000 * attempt);
        }
      }
    }
  } catch (err) {
    console.error(`Could not set webhook for ${user.email}: ${err.message}`);
  }
}

// ==================== LAUNCH ALL BOTS ON STARTUP ====================
async function launchAllBots() {
  const users = await User.find({ telegramBotToken: { $exists: true, $ne: null } }).lean();
  for (const user of users) launchUserBot(user);
  console.log(`✅ Launched ${users.length} bot(s)`);
}

// ==================== CLEAR WEBHOOK VIA HTTP ====================
async function clearWebhookFor(token) {
  if (!token) return;
  try {
    await axios.post(
      `https://api.telegram.org/bot${token}/deleteWebhook`,
      { drop_pending_updates: true },
      { timeout: 15000 }
    );
  } catch (err) {
    console.warn(`Could not clear webhook: ${err.message}`);
  }
}

// ==================== VALIDATE BOT TOKEN ====================
async function validateBotToken(token) {
  const MAX = 7;
  for (let attempt = 1; attempt <= MAX; attempt++) {
    try {
      const res = await axios.get(`https://api.telegram.org/bot${token}/getMe`, { timeout: 15000 });
      if (!res.data.ok) {
        throw Object.assign(new Error(res.data.description || 'Telegram rejected token'), { statusCode: 400 });
      }
      const botInfo = res.data.result;
      if (!botInfo?.username) {
        throw Object.assign(new Error('Missing bot username in Telegram response'), { statusCode: 400 });
      }
      return botInfo;
    } catch (err) {
      if (err.statusCode === 400) throw err;  // Bad token — don't retry
      console.warn(`Bot token validation attempt ${attempt}/${MAX}: ${err.message}`);
      if (attempt >= MAX) {
        throw Object.assign(new Error('Network error validating bot token. Please try again later.'), { statusCode: 502 });
      }
      await _sleep(8000);
    }
  }
}

// ==================== SEND 2FA CODE ====================
async function send2FACode(user, code) {
  if (!user.isTelegramConnected || !user.telegramChatId || !activeBots.has(user.id)) return false;
  try {
    await activeBots.get(user.id).telegram.sendMessage(
      user.telegramChatId,
      `Security Alert – Password Reset\n\nYour 6-digit code:\n\n<b>${code}</b>\n\nValid for 10 minutes.`,
      { parse_mode: 'HTML' }
    );
    return true;
  } catch (err) {
    console.error('Failed to send 2FA code:', err.message);
    return false;
  }
}

function _sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

module.exports = {
  activeBots,
  pendingSubscribers,
  launchUserBot,
  launchAllBots,
  clearWebhookFor,
  validateBotToken,
  send2FACode
};
