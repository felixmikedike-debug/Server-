'use strict';

const fp             = require('fastify-plugin');
const { v4: uuidv4 } = require('uuid');

const { ScheduledBroadcast }      = require('../models');
const { getUserLimits, prepareTelegramMessage } = require('../utils');
const { adminSettingsCache }      = require('../adminSettings');
const { broadcastQueue, incrementDailyBroadcast } = require('../worker');
const { MAX_MSG_LENGTH }          = require('../config');

const MAX_RAW = MAX_MSG_LENGTH * 10;

// ==================== SCHEMAS ====================
const broadcastNowSchema = {
  body: {
    type: 'object',
    required: ['message'],
    properties: {
      message: { type: 'string', minLength: 1, maxLength: MAX_RAW }
    }
  }
};

const scheduleSchema = {
  body: {
    type: 'object',
    required: ['message', 'scheduledTime'],
    properties: {
      message:       { type: 'string', minLength: 1, maxLength: MAX_RAW },
      scheduledTime: { type: 'string', minLength: 1 },
      recipients:    { type: 'string', enum: ['all'], default: 'all' }
    }
  }
};

const editSchema = {
  body: {
    type: 'object',
    properties: {
      message:       { type: 'string', minLength: 1, maxLength: MAX_RAW },
      scheduledTime: { type: 'string', minLength: 1 },
      recipients:    { type: 'string', enum: ['all'] }
    }
  }
};

// ==================== HELPERS ====================
function parseAndValidateFutureTime(raw) {
  const t = new Date(raw);
  if (isNaN(t.getTime()) || t <= new Date()) return null;
  return t;
}

// ==================== PLUGIN ====================
module.exports = fp(async function broadcastRoutes(fastify) {
  await fastify.register(require('../middleware/auth'));

  // POST /api/broadcast/now
  fastify.post('/now', { schema: broadcastNowSchema, preHandler: fastify.authenticate }, async (req, reply) => {
    const limits  = getUserLimits(req.user, adminSettingsCache);
    const todayN  = await incrementDailyBroadcast(req.user.id);

    if (limits.dailyBroadcasts !== Infinity && todayN > limits.dailyBroadcasts) {
      return reply.status(429).send({ error: `Daily broadcast limit of ${limits.dailyBroadcasts} reached. Upgrade to send more.` });
    }

    const readyMsg = prepareTelegramMessage(req.body.message.trim());
    if (!readyMsg.length) {
      return reply.status(400).send({ error: 'Message is empty after processing.' });
    }

    await broadcastQueue.add('send-broadcast', { userId: req.user.id, message: readyMsg });

    return reply.send({
      success: true,
      message: 'Broadcast queued. You will receive a delivery report via Telegram shortly.'
    });
  });

  // POST /api/broadcast/schedule
  fastify.post('/schedule', { schema: scheduleSchema, preHandler: fastify.authenticate }, async (req, reply) => {
    const limits = getUserLimits(req.user, adminSettingsCache);
    const todayN = await incrementDailyBroadcast(req.user.id);

    if (limits.dailyBroadcasts !== Infinity && todayN > limits.dailyBroadcasts) {
      return reply.status(429).send({ error: `Daily broadcast limit of ${limits.dailyBroadcasts} reached.` });
    }

    const time = parseAndValidateFutureTime(req.body.scheduledTime);
    if (!time) return reply.status(400).send({ error: 'scheduledTime must be a valid future datetime.' });

    const readyMsg    = prepareTelegramMessage(req.body.message.trim());
    if (!readyMsg.length) return reply.status(400).send({ error: 'Message is empty after processing.' });

    const broadcastId = uuidv4();
    await ScheduledBroadcast.create({
      broadcastId,
      userId:        req.user.id,
      message:       readyMsg,
      recipients:    req.body.recipients || 'all',
      scheduledTime: time,
      status:        'pending'
    });

    const delay = time.getTime() - Date.now();
    await broadcastQueue.add('send-broadcast', {
      userId: req.user.id, message: readyMsg, broadcastId
    }, {
      jobId: broadcastId,
      delay: Math.max(0, delay)
    });

    return reply.send({ success: true, broadcastId, scheduledTime: time.toISOString() });
  });

  // GET /api/broadcast/scheduled
  fastify.get('/scheduled', { preHandler: fastify.authenticate }, async (req, reply) => {
    const scheduled = await ScheduledBroadcast
      .find({ userId: req.user.id, status: 'pending' })
      .sort({ scheduledTime: 1 })
      .lean();

    return reply.send({
      success: true,
      scheduled: scheduled.map(s => ({
        broadcastId:   s.broadcastId,
        message:       s.message.slice(0, 100) + (s.message.length > 100 ? '…' : ''),
        scheduledTime: s.scheduledTime.toISOString(),
        status:        s.status,
        recipients:    s.recipients
      }))
    });
  });

  // GET /api/broadcast/scheduled/:broadcastId/details
  fastify.get('/scheduled/:broadcastId/details', { preHandler: fastify.authenticate }, async (req, reply) => {
    const task = await ScheduledBroadcast.findOne({
      broadcastId: req.params.broadcastId,
      userId:      req.user.id,
      status:      'pending'
    }).lean();

    if (!task) return reply.status(404).send({ error: 'Broadcast not found or not editable.' });

    // Convert to local datetime-local format for front-end
    const d       = new Date(task.scheduledTime);
    const offset  = d.getTimezoneOffset() * 60000;
    const local   = new Date(d.getTime() - offset).toISOString().slice(0, 16);

    return reply.send({
      success:       true,
      message:       task.message,
      scheduledTime: local,
      recipients:    task.recipients || 'all'
    });
  });

  // PATCH /api/broadcast/scheduled/:broadcastId
  fastify.patch('/scheduled/:broadcastId', { schema: editSchema, preHandler: fastify.authenticate }, async (req, reply) => {
    const task = await ScheduledBroadcast.findOne({
      broadcastId: req.params.broadcastId,
      userId:      req.user.id,
      status:      'pending'
    });
    if (!task) return reply.status(404).send({ error: 'Broadcast not found or not pending.' });

    // Remove old queued job
    const oldJob = await broadcastQueue.getJob(task.broadcastId);
    if (oldJob) await oldJob.remove();

    let changed = false;

    if (req.body.message?.trim()) {
      const ready = prepareTelegramMessage(req.body.message.trim());
      if (!ready.length) return reply.status(400).send({ error: 'Message is empty after processing.' });
      task.message = ready;
      changed = true;
    }
    if (req.body.recipients) {
      task.recipients = req.body.recipients;
      changed = true;
    }
    if (req.body.scheduledTime) {
      const newTime = parseAndValidateFutureTime(req.body.scheduledTime);
      if (!newTime) return reply.status(400).send({ error: 'scheduledTime must be a valid future datetime.' });
      task.scheduledTime = newTime;
      changed = true;
    }

    if (changed) {
      await task.save();
      const delay = Math.max(0, task.scheduledTime.getTime() - Date.now());
      await broadcastQueue.add('send-broadcast', {
        userId: task.userId, message: task.message, broadcastId: task.broadcastId
      }, { jobId: task.broadcastId, delay });
    }

    return reply.send({ success: true, broadcastId: task.broadcastId, scheduledTime: task.scheduledTime.toISOString() });
  });

  // DELETE /api/broadcast/scheduled/:broadcastId
  fastify.delete('/scheduled/:broadcastId', { preHandler: fastify.authenticate }, async (req, reply) => {
    const task = await ScheduledBroadcast.findOne({
      broadcastId: req.params.broadcastId,
      userId:      req.user.id
    });
    if (!task) return reply.status(404).send({ error: 'Broadcast not found.' });

    const job = await broadcastQueue.getJob(task.broadcastId);
    if (job) await job.remove();

    await task.deleteOne();
    return reply.send({ success: true });
  });
});
