'use strict';

const mongoose = require('mongoose');
const { Schema } = mongoose;

// ==================== USER ====================
const userSchema = new Schema({
  id:                       { type: String, required: true, unique: true, index: true },
  fullName:                 { type: String, required: true, trim: true, maxlength: 120 },
  email:                    { type: String, required: true, unique: true, lowercase: true, trim: true },
  password:                 { type: String, required: true },
  telegramBotToken:         { type: String, default: null, index: true, sparse: true },
  telegramChatId:           { type: String, default: null },
  isTelegramConnected:      { type: Boolean, default: false },
  botUsername:              { type: String, default: null },
  isSubscribed:             { type: Boolean, default: false },
  subscriptionEndDate:      { type: Date, default: null },
  subscriptionPlan:         { type: String, default: null },
  pendingPaymentReference:  { type: String, default: null }
}, { timestamps: true });

// ==================== LANDING PAGE ====================
const landingPageSchema = new Schema({
  shortId: { type: String, required: true, unique: true, index: true },
  userId:  { type: String, required: true, index: true },
  title:   { type: String, required: true, trim: true, maxlength: 200 },
  config:  { type: Schema.Types.Mixed, required: true }
}, { timestamps: true });

// ==================== FORM PAGE ====================
const formPageSchema = new Schema({
  shortId:        { type: String, required: true, unique: true, index: true },
  userId:         { type: String, required: true, index: true },
  title:          { type: String, required: true, trim: true, maxlength: 200 },
  state:          { type: Schema.Types.Mixed, required: true },
  welcomeMessage: { type: String, default: '' }
}, { timestamps: true });

// ==================== CONTACT ====================
const contactSchema = new Schema({
  userId:         { type: String, required: true },
  shortId:        { type: String, default: null },
  name:           { type: String, trim: true, maxlength: 120 },
  contact:        { type: String, required: true },
  telegramChatId: { type: String, default: null },
  status:         { type: String, enum: ['pending', 'subscribed', 'unsubscribed'], default: 'pending' },
  submittedAt:    { type: Date, default: null },
  subscribedAt:   { type: Date, default: null },
  unsubscribedAt: { type: Date, default: null }
}, { timestamps: true });

contactSchema.index({ userId: 1 });
contactSchema.index({ userId: 1, contact: 1 });
contactSchema.index({ userId: 1, telegramChatId: 1 });
contactSchema.index({ userId: 1, status: 1 });

// ==================== SCHEDULED BROADCAST ====================
const scheduledBroadcastSchema = new Schema({
  broadcastId:   { type: String, required: true, unique: true, index: true },
  userId:        { type: String, required: true, index: true },
  message:       { type: String, required: true },
  recipients:    { type: String, default: 'all' },
  scheduledTime: { type: Date, required: true, index: true },
  status:        { type: String, enum: ['pending', 'processing', 'done', 'failed'], default: 'pending', index: true }
}, { timestamps: true });

// ==================== BROADCAST DAILY COUNTER ====================
const broadcastDailySchema = new Schema({
  userId: { type: String, required: true },
  date:   { type: String, required: true },
  count:  { type: Number, default: 1 }
}, { timestamps: true });

broadcastDailySchema.index({ userId: 1, date: 1 }, { unique: true });

// ==================== ADMIN SETTINGS ====================
const adminSettingsSchema = new Schema({
  dailyBroadcastLimit: { type: Number, default: 3, min: 1 },
  maxLandingPages:     { type: Number, default: 5, min: 1 },
  maxForms:            { type: Number, default: 5, min: 1 }
}, { timestamps: true });

adminSettingsSchema.statics.getSettings = async function () {
  let s = await this.findOne().lean();
  if (!s) s = await this.create({ dailyBroadcastLimit: 3, maxLandingPages: 5, maxForms: 5 });
  return s;
};

adminSettingsSchema.statics.updateSettings = async function (updates) {
  return this.findOneAndUpdate({}, updates, { new: true, upsert: true, runValidators: true });
};

// ==================== EXPORTS ====================
exports.User               = mongoose.model('User',               userSchema);
exports.LandingPage        = mongoose.model('LandingPage',        landingPageSchema);
exports.FormPage           = mongoose.model('FormPage',           formPageSchema);
exports.Contact            = mongoose.model('Contact',            contactSchema);
exports.ScheduledBroadcast = mongoose.model('ScheduledBroadcast', scheduledBroadcastSchema);
exports.BroadcastDaily     = mongoose.model('BroadcastDaily',     broadcastDailySchema);
exports.AdminSettings      = mongoose.model('AdminSettings',      adminSettingsSchema);
