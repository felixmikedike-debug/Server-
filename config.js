'use strict';

const crypto = require('crypto');

// ==================== SECRETS ====================
const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET || JWT_SECRET.length < 32) {
  console.warn('⚠️  WARNING: JWT_SECRET not set or too short — using a random ephemeral secret. Sessions will not survive restarts!');
}
exports.JWT_SECRET = JWT_SECRET || crypto.randomBytes(48).toString('hex');

const PAYSTACK_SECRET_KEY = process.env.PAYSTACK_SECRET_KEY;
if (!PAYSTACK_SECRET_KEY || PAYSTACK_SECRET_KEY.startsWith('sk_test_fallback')) {
  console.warn('⚠️  WARNING: PAYSTACK_SECRET_KEY not set.');
}
exports.PAYSTACK_SECRET_KEY = PAYSTACK_SECRET_KEY || '';

exports.ADMIN_PASSWORD = process.env.ADMIN_PASSWORD;
if (!exports.ADMIN_PASSWORD) {
  console.error('FATAL: ADMIN_PASSWORD env var is not set.');
  process.exit(1);
}

exports.DOMAIN = process.env.DOMAIN;

let WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;
if (!WEBHOOK_SECRET || !WEBHOOK_SECRET.trim()) {
  WEBHOOK_SECRET = crypto.randomBytes(32).toString('hex');
  console.warn('⚠️  WARNING: WEBHOOK_SECRET not set — generated ephemeral one. Add to .env:');
  console.warn(`     WEBHOOK_SECRET=${WEBHOOK_SECRET}`);
} else {
  console.log('Webhook secret loaded from .env');
}
exports.WEBHOOK_SECRET = WEBHOOK_SECRET;

exports.MONGODB_URI   = process.env.MONGODB_URI || 'mongodb://localhost:27017/sendm';
exports.PORT          = parseInt(process.env.PORT, 10) || 3000;

// ==================== BUSINESS CONSTANTS ====================
exports.MONTHLY_PRICE_KOBO  = 150000;   // ₦1,500 in kobo
exports.BATCH_SIZE          = 25;
exports.BATCH_INTERVAL_MS   = 8000;
exports.MAX_MSG_LENGTH       = 4000;

// TTL (ms)
exports.TTL = {
  pages:    5  * 60 * 1000,
  forms:    5  * 60 * 1000,
  contacts: 2  * 60 * 1000,
  public:   10 * 60 * 1000
};

exports.CONTACT_REGEX = /^(\+?[0-9\s\-()]{7,20}|[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$/;
