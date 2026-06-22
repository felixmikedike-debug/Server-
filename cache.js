'use strict';

const { TTL } = require('./config');

// ==================== PER-USER CACHE ====================
const userCache = new Map();

function getUserCache(userId) {
  let bucket = userCache.get(userId);
  if (!bucket) {
    bucket = {
      pages: null, pagesTs: 0,
      forms: null, formsTs: 0,
      contacts: null, contactsTs: 0,
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
  if (type === 'pages'    || type === 'all') { bucket.pages    = null; bucket.pagesTs    = 0; }
  if (type === 'forms'    || type === 'all') { bucket.forms    = null; bucket.formsTs    = 0; }
  if (type === 'contacts' || type === 'all') { bucket.contacts = null; bucket.contactsTs = 0; }
  bucket.lastAccess = Date.now();
}

// ==================== PUBLIC CACHE ====================
const publicCache = new Map();

function getPublicCache(key) {
  const entry = publicCache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.timestamp > TTL.public) {
    publicCache.delete(key);
    return null;
  }
  return entry.data;
}

function setPublicCache(key, data) {
  publicCache.set(key, { data, timestamp: Date.now() });
}

function invalidatePublicCache(key) {
  publicCache.delete(key);
}

// ==================== PERIODIC CLEANUP ====================
const INACTIVE_THRESHOLD = 30 * 60 * 1000;

const _cleanupInterval = setInterval(() => {
  const now = Date.now();

  for (const [key, val] of publicCache.entries()) {
    if (now - val.timestamp > TTL.public) {
      publicCache.delete(key);
    }
  }

  for (const [userId, bucket] of userCache.entries()) {
    if (now - bucket.lastAccess > INACTIVE_THRESHOLD) {
      userCache.delete(userId);
    }
  }
}, 10 * 60 * 1000);

// Don't block process exit
_cleanupInterval.unref();

module.exports = {
  getUserCache,
  invalidateUserCache,
  getPublicCache,
  setPublicCache,
  invalidatePublicCache
};
