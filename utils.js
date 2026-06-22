'use strict';

const { MAX_MSG_LENGTH } = require('./config');

// ==================== HTML ESCAPING ====================
exports.escapeHtml = function escapeHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
};

// ==================== TELEGRAM HTML SANITIZER ====================
const ALLOWED_TAGS = new Set([
  'b', 'strong', 'i', 'em', 'u', 'ins', 's', 'strike', 'del',
  'span', 'tg-spoiler', 'a', 'code', 'pre', 'tg-emoji', 'blockquote'
]);

const ALLOWED_ATTRS = {
  a:           ['href'],
  'tg-emoji':  ['emoji-id'],
  blockquote:  ['expandable']
};

exports.sanitizeTelegramHtml = function sanitizeTelegramHtml(unsafe) {
  if (!unsafe || typeof unsafe !== 'string') return '';

  // Strip dangerous blocks first
  let clean = unsafe
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
    .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, '')
    .replace(/on\w+\s*=\s*"[^"]*"/gi, '')
    .replace(/on\w+\s*=\s*'[^']*'/gi, '')
    .replace(/javascript:/gi, '');

  clean = clean.replace(/<\/?([a-z][a-z0-9-]*)\b[^>]*>/gi, (match, tagName) => {
    const tag = tagName.toLowerCase();
    if (!ALLOWED_TAGS.has(tag)) return '';
    if (match.startsWith('</')) return `</${tag}>`;

    let attrs = '';
    const attrRegex = /([a-z0-9-]+)\s*=\s*"([^"]*)"/gi;
    let m;
    while ((m = attrRegex.exec(match)) !== null) {
      const attrName  = m[1].toLowerCase();
      let   attrValue = m[2];
      if (ALLOWED_ATTRS[tag]?.includes(attrName)) {
        if (attrName === 'href' && !/^https?:\/\//i.test(attrValue) && !attrValue.startsWith('/')) {
          attrValue = '#';
        }
        attrs += ` ${attrName}="${attrValue.replace(/"/g, '&quot;')}"`;
      }
    }
    return `<${tag}${attrs}>`;
  });

  return clean.trim();
};

// ==================== MESSAGE PREPARATION ====================
exports.prepareTelegramMessage = function prepareTelegramMessage(raw) {
  if (!raw || typeof raw !== 'string') return '';
  let msg = raw.trim()
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>\s*<p[^>]*>/gi, '\n\n')
    .replace(/<p[^>]*>/gi, '')
    .replace(/<\/p>/gi, '\n')
    .replace(/<div[^>]*>/gi, '\n')
    .replace(/<\/div>/gi, '\n')
    .replace(/\n{3,}/g, '\n\n');
  return exports.sanitizeTelegramHtml(msg);
};

// ==================== MESSAGE SPLITTER ====================
exports.splitTelegramMessage = function splitTelegramMessage(text) {
  if (!text) return [];
  const chunks = [];
  let current = '';
  const lines = text.split(/\r?\n/);

  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    while (line.length > MAX_MSG_LENGTH) {
      if (current) { chunks.push(current.trim()); current = ''; }
      chunks.push(line.slice(0, MAX_MSG_LENGTH).trim());
      line = line.slice(MAX_MSG_LENGTH);
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
  return chunks.map((chunk, i) => {
    const header = `(${i + 1}/${total})\n\n`;
    return header.length + chunk.length > MAX_MSG_LENGTH ? chunk : header + chunk;
  });
};

// ==================== TEXT → HTML (display) ====================
exports.textToHtmlForDisplay = function textToHtmlForDisplay(text) {
  if (!text) return '';
  return text
    .replace(/\n{2,}/g, '</p><p>')
    .replace(/\n/g, '<br>');
};

// ==================== DATE HELPERS ====================
exports.getTodayDateString = function getTodayDateString() {
  return new Date().toISOString().slice(0, 10);
};

// ==================== SUBSCRIPTION HELPERS ====================
exports.hasActiveSubscription = function hasActiveSubscription(user) {
  return !!(user.isSubscribed && user.subscriptionEndDate && new Date(user.subscriptionEndDate) > new Date());
};

exports.getUserLimits = function getUserLimits(user, adminCache) {
  if (exports.hasActiveSubscription(user)) {
    return { dailyBroadcasts: Infinity, maxLandingPages: Infinity, maxForms: Infinity };
  }
  return {
    dailyBroadcasts:  adminCache.dailyBroadcastLimit,
    maxLandingPages:  adminCache.maxLandingPages,
    maxForms:         adminCache.maxForms
  };
};

// ==================== 2FA CODE ====================
exports.generate2FACode = function generate2FACode() {
  return Math.floor(100000 + Math.random() * 900000).toString();
};
