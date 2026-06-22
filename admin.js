'use strict';

const fp = require('fastify-plugin');

const { User, AdminSettings } = require('../models');
const { adminSettingsCache }  = require('../adminSettings');
const { ADMIN_PASSWORD }      = require('../config');

// ==================== HTML TEMPLATES ====================
function panelHtml({ totalUsers, payingUsers, cache, error }) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Admin Panel</title>
  <style>
    *{box-sizing:border-box}
    body{font-family:'Segoe UI',sans-serif;background:#121212;color:#e0e0e0;display:flex;
         justify-content:center;align-items:center;min-height:100vh;margin:0;padding:20px}
    .container{background:#1e1e1e;padding:40px;border-radius:12px;
               box-shadow:0 8px 32px rgba(0,0,0,.6);width:100%;max-width:600px}
    h1{text-align:center;color:#ffd700;margin-bottom:30px}
    .stats{display:grid;grid-template-columns:1fr 1fr;gap:15px;margin-bottom:30px}
    .stat-box{background:#2d2d2d;padding:20px;border-radius:10px;text-align:center}
    .stat-number{font-size:2.5em;font-weight:bold;color:#00ff41;margin:10px 0}
    .stat-label{color:#aaa}
    label{display:block;margin:20px 0 8px;font-size:1.05em}
    input[type=number],input[type=password]{width:100%;padding:12px;background:#2d2d2d;
      border:1px solid #444;border-radius:6px;color:#fff;font-size:1em}
    input:focus{outline:none;border-color:#ffd700}
    button{width:100%;padding:14px;background:#ffd700;color:#000;font-weight:bold;
           border:none;border-radius:6px;cursor:pointer;font-size:1.1em;margin-top:20px}
    button:hover{background:#e6c200}
    .current{text-align:center;margin:25px 0;padding:15px;background:#2d2d2d;border-radius:8px}
    .error{color:#f44336;text-align:center;margin-bottom:16px;font-weight:bold}
  </style>
</head>
<body>
  <div class="container">
    <h1>⚙️ Admin Panel</h1>
    <div class="stats">
      <div class="stat-box">
        <div class="stat-number">${totalUsers}</div>
        <div class="stat-label">Total Users</div>
      </div>
      <div class="stat-box">
        <div class="stat-number">${payingUsers}</div>
        <div class="stat-label">Paying Users</div>
      </div>
    </div>
    ${error ? `<p class="error">${error}</p>` : ''}
    <form method="POST" action="/admin-limits">
      <label>Owner Password</label>
      <input type="password" name="password" required placeholder="Enter admin password">
      <label>Daily Broadcasts per Free User</label>
      <input type="number" name="daily_broadcast" min="1" max="1000" value="${cache.dailyBroadcastLimit}" required>
      <label>Max Landing Pages per Free User</label>
      <input type="number" name="max_pages" min="1" max="1000" value="${cache.maxLandingPages}" required>
      <label>Max Forms per Free User</label>
      <input type="number" name="max_forms" min="1" max="1000" value="${cache.maxForms}" required>
      <div class="current">
        <strong>Current Free Limits:</strong><br>
        Broadcasts/day: <b>${cache.dailyBroadcastLimit}</b> &nbsp;|&nbsp;
        Pages: <b>${cache.maxLandingPages}</b> &nbsp;|&nbsp;
        Forms: <b>${cache.maxForms}</b>
      </div>
      <button type="submit">Update Limits</button>
    </form>
  </div>
</body>
</html>`;
}

function successHtml({ daily, pages, forms }) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Updated</title>
  <style>
    body{font-family:'Segoe UI',sans-serif;background:#121212;color:#e0e0e0;display:flex;
         justify-content:center;align-items:center;min-height:100vh;margin:0}
    .box{background:#1e1e1e;padding:40px;border-radius:12px;text-align:center;max-width:500px}
    h1{color:#4caf50}
    a{color:#ffd700;text-decoration:none;font-weight:bold}
    a:hover{text-decoration:underline}
  </style>
</head>
<body>
  <div class="box">
    <h1>✓ Limits Updated</h1>
    <p>Daily Broadcasts: <strong>${daily}</strong><br>
       Max Pages: <strong>${pages}</strong><br>
       Max Forms: <strong>${forms}</strong></p>
    <p><a href="/admin-limits">← Back to Panel</a></p>
  </div>
</body>
</html>`;
}

function errorHtml(msg) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Error</title>
  <style>
    body{font-family:sans-serif;background:#121212;color:#f44336;display:flex;
         justify-content:center;align-items:center;min-height:100vh;text-align:center}
  </style>
</head>
<body><h1>${msg}</h1><br><a href="/admin-limits" style="color:#ffd700">← Back</a></body>
</html>`;
}

// ==================== PLUGIN ====================
module.exports = fp(async function adminRoutes(fastify) {

  // GET /admin-limits
  fastify.get('/admin-limits', {
    config: { rateLimit: { max: 30, timeWindow: '1 minute' } }
  }, async (req, reply) => {
    const [totalUsers, payingUsers] = await Promise.all([
      User.countDocuments({}),
      User.countDocuments({ isSubscribed: true, subscriptionEndDate: { $gt: new Date() } })
    ]);

    return reply
      .type('text/html')
      .send(panelHtml({ totalUsers, payingUsers, cache: adminSettingsCache }));
  });

  // POST /admin-limits
  fastify.post('/admin-limits', {
    config: { rateLimit: { max: 10, timeWindow: '1 minute' } }
  }, async (req, reply) => {
    const { password, daily_broadcast, max_pages, max_forms } = req.body;

    if (!password || password !== ADMIN_PASSWORD) {
      return reply.status(403).type('text/html').send(errorHtml('Access Denied — Wrong password'));
    }

    const newDaily = parseInt(daily_broadcast, 10);
    const newPages = parseInt(max_pages, 10);
    const newForms = parseInt(max_forms, 10);

    if ([newDaily, newPages, newForms].some(v => isNaN(v) || v < 1 || v > 10000)) {
      return reply.status(400).type('text/html').send(errorHtml('Invalid values — all limits must be between 1 and 10,000'));
    }

    const updates = { dailyBroadcastLimit: newDaily, maxLandingPages: newPages, maxForms: newForms };

    await AdminSettings.updateSettings(updates);

    // Sync in-memory cache
    adminSettingsCache.dailyBroadcastLimit = newDaily;
    adminSettingsCache.maxLandingPages     = newPages;
    adminSettingsCache.maxForms            = newForms;

    req.log.info({ updates }, 'Admin limits updated');

    return reply.type('text/html').send(successHtml({ daily: newDaily, pages: newPages, forms: newForms }));
  });
});
