'use strict';

const { Queue, Worker }         = require('bullmq');
const { redisConnection }       = require('./redis');
const { Contact, ScheduledBroadcast, User, BroadcastDaily } = require('./models');
const { activeBots }            = require('./bots');
const { splitTelegramMessage }  = require('./utils');
const { getTodayDateString }    = require('./utils');
const { BATCH_SIZE, BATCH_INTERVAL_MS } = require('./config');

const QUEUE_NAME = 'telegram-broadcasts';

const broadcastQueue = new Queue(QUEUE_NAME, {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 4,
    backoff: { type: 'exponential', delay: 5000 },
    removeOnComplete: { count: 200 },
    removeOnFail:     { count: 100 }
  }
});

// ==================== PROCESSOR ====================
async function processBroadcast(job) {
  const { userId, message, broadcastId } = job.data;

  const bot = activeBots.get(userId);
  if (!bot) throw new Error(`No active bot for user ${userId}`);

  const chunks  = splitTelegramMessage(message);
  const targets = await Contact.find({
    userId,
    status: 'subscribed',
    telegramChatId: { $exists: true, $ne: null }
  }).lean();

  let sent = 0, failed = 0;
  const total = targets.length;

  // Process in batches
  for (let b = 0; b < targets.length; b += BATCH_SIZE) {
    const batch = targets.slice(b, b + BATCH_SIZE);

    await Promise.all(batch.map(async (target) => {
      try {
        for (const chunk of chunks) {
          await bot.telegram.sendMessage(target.telegramChatId, chunk, { parse_mode: 'HTML' });
        }
        sent++;
      } catch (err) {
        failed++;
        const isBlocked = err?.response?.error_code === 403
          || /blocked|forbidden|chat not found|deactivated/i.test(err.message || '');
        if (isBlocked) {
          await Contact.findByIdAndUpdate(target._id, {
            status: 'unsubscribed',
            unsubscribedAt: new Date(),
            telegramChatId: null
          });
        }
      }
    }));

    if (b + BATCH_SIZE < targets.length) {
      await _sleep(BATCH_INTERVAL_MS);
    }
  }

  // Send delivery report to user
  await _sendReport(userId, bot, { total, sent, failed, broadcastId });

  // Clean up scheduled record
  if (broadcastId) {
    await ScheduledBroadcast.deleteOne({ broadcastId });
  }
}

async function _sendReport(userId, bot, { total, sent, failed, broadcastId }) {
  try {
    const user = await User.findOne({ id: userId }).lean();
    if (!user?.isTelegramConnected || !user.telegramChatId) return;

    const header = broadcastId ? '<b>Scheduled Broadcast Report</b>\n\n' : '<b>Broadcast Report</b>\n\n';
    let body;
    if (total === 0) {
      body = 'No subscribed contacts with Telegram connected.';
    } else {
      const emoji = failed === 0 ? '✅' : '⚠️';
      body = `${emoji} <b>${sent} of ${total}</b> delivered.`;
      if (failed > 0) body += `\n${failed} failed.`;
    }
    body += `\n\nTime: ${new Date().toLocaleString()}`;

    await bot.telegram.sendMessage(user.telegramChatId, header + body, { parse_mode: 'HTML' });
  } catch (err) {
    console.error(`Failed to send broadcast report to user ${userId}:`, err.message);
  }
}

// ==================== WORKER ====================
const worker = new Worker(QUEUE_NAME, processBroadcast, {
  connection: redisConnection,
  concurrency: 4
});

worker.on('completed', (job) => {
  console.log(`Broadcast job ${job.id} completed for user ${job.data.userId}`);
});

worker.on('failed', async (job, err) => {
  console.error(`Broadcast job ${job?.id} permanently failed: ${err.message}`);
  const { userId, broadcastId } = job?.data || {};

  if (broadcastId) {
    await ScheduledBroadcast.findOneAndUpdate({ broadcastId }, { status: 'failed' }).catch(() => {});
  }

  // Notify user of failure
  const bot = activeBots.get(userId);
  const user = await User.findOne({ id: userId }).lean().catch(() => null);
  if (bot && user?.isTelegramConnected && user.telegramChatId) {
    const label = broadcastId ? 'Scheduled Broadcast' : 'Broadcast';
    try {
      await bot.telegram.sendMessage(
        user.telegramChatId,
        `<b>${label} Failed</b>\n\nFailed after all retries.\nError: ${escapeText(err.message)}`,
        { parse_mode: 'HTML' }
      );
    } catch { /* best-effort */ }
  }
});

// ==================== RECOVERY ON RESTART ====================
async function recoverScheduled() {
  console.log('🔄 Recovering scheduled broadcasts after restart…');
  const now    = new Date();
  const pending = await ScheduledBroadcast.find({
    status:        'pending',
    scheduledTime: { $gt: now }
  }).lean();

  if (!pending.length) {
    console.log('✓ No pending scheduled broadcasts to recover');
    return;
  }

  let recovered = 0, skipped = 0;

  for (const task of pending) {
    const existing = await broadcastQueue.getJob(task.broadcastId);
    if (existing) { skipped++; continue; }

    const delayMs = Math.max(0, task.scheduledTime.getTime() - Date.now());
    await broadcastQueue.add('send-broadcast', {
      userId:      task.userId,
      message:     task.message,
      broadcastId: task.broadcastId
    }, {
      jobId: task.broadcastId,
      delay: delayMs
    });
    recovered++;
  }

  console.log(`✓ Recovery: ${recovered} re-queued, ${skipped} already in queue`);
}

// ==================== DAILY COUNTER ====================
async function incrementDailyBroadcast(userId) {
  const today  = getTodayDateString();
  const record = await BroadcastDaily.findOneAndUpdate(
    { userId, date: today },
    { $inc: { count: 1 } },
    { upsert: true, new: true }
  );
  return record.count;
}

function escapeText(str) {
  return String(str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function _sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

module.exports = { broadcastQueue, worker, recoverScheduled, incrementDailyBroadcast };
