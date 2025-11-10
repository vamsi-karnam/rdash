/*
  -----------------------------------------------------------------------------
  Author: Vamsi Karnam
  Description: Webpage interaction and functionality
  -----------------------------------------------------------------------------
*/

// Token auth helpers
function sanitizeToken(raw) {
  if (!raw) return '';
  let t = String(raw).trim();
  t = t.replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1');
  return t;
}
function getToken() { return sanitizeToken(localStorage.getItem('urdaf_token') || ''); }
function setToken(token) {
  const clean = sanitizeToken(token);
  localStorage.setItem('urdaf_token', clean);
  setTokenCookie(clean);
}
function clearToken() { localStorage.removeItem('urdaf_token'); clearTokenCookie(); }
function getHeaders() { const t = getToken(); return t ? { 'Authorization': 'Bearer ' + t } : {}; }
function setTokenCookie(token) {
  const isHttps = location.protocol === 'https:';
  document.cookie = [
    `urdaf_token=${encodeURIComponent(token)}`,
    'Path=/',
    'SameSite=Lax',
    isHttps ? 'Secure' : ''
  ].filter(Boolean).join('; ');
}
function clearTokenCookie() { document.cookie = 'urdaf_token=; Path=/; Max-Age=0; SameSite=Lax'; }

function fmtClock(ms) {
  const d = new Date(ms);
  // hh:mm:ss with zero padding
  const hh = String(d.getHours()).padStart(2,'0');
  const mm = String(d.getMinutes()).padStart(2,'0');
  const ss = String(d.getSeconds()).padStart(2,'0');
  return `${hh}:${mm}:${ss}`;
}

let _ddsPrimed = false;

async function refreshDdsDrawerIfNeeded(robot) {
  if (!_ddsPrimed || !ddsDrawerEl) return;
  const robotsData = await fetchJSON('/api/robots');
  const meta = robotsData.robots?.[robot] || { sensors: [], texts: [] };
  const ddsSensors = (meta.sensors || []).filter(isDdsSensorName);
  const ddsTexts   = (meta.texts   || []).filter(isDdsSensorName);
  const types      = await fetchJSON(`/api/types/${encodeURIComponent(robot)}`);
  const statuses   = await fetchJSON(`/api/status/${encodeURIComponent(robot)}`);
  await rebuildDdsDrawerFor(robot, ddsSensors, ddsTexts, types, statuses);
}


function setStatus(text) { const pill = document.getElementById('status-pill'); if (pill) pill.textContent = text; }
window.saveTokenAndReconnect = function(token){ setToken(token || ''); reconnectAll(); };
window.clearToken = function(){ clearToken(); reconnectAll(); };
function reconnectAll(){ if (ws && ws.connected) ws.disconnect(); ensureSocket(); navigateOverview(); }

// Modal helpers
function hideGate(){ const g=document.getElementById('auth-gate'); if(g) g.hidden = true; }
function showGate(){ const g=document.getElementById('auth-gate'); if(g) g.hidden = false; }

// State 
let ws = null;
const chartsBySensor = new Map();   // key -> { ec, seriesIdByMetric, paused, windowSec, leadSec, ... }
const pendingUpdates = new Map();   // key -> { t, data:{} }
let rafScheduled = false;
let currentRobot = null;

// Track current DDS items so we don't rebuild drawer every tick
const ddsIndexByRobot = new Map(); // robot -> { sensors:Set, texts:Set }

function _ensureDdsIndex(robot) {
  let idx = ddsIndexByRobot.get(robot);
  if (!idx) {
    idx = { sensors: new Set(), texts: new Set() };
    ddsIndexByRobot.set(robot, idx);
  }
  return idx;
}

// Text logs state
const logsBySensor = new Map();     // key -> { containerEl, paused, follow, lines }

// Sliding window params
const WINDOW_SEC = 10;      // visible span
const LEAD_SEC   = 6;       // keep latest a bit right of center
const MAX_POINTS = 2000;    // client cap

// Paused buffering safety
const MAX_BUFFER_POINTS   = 200;     // per metric cap while paused
const REFILL_THRESHOLD_MS = 5000;    // if paused > this, refetch history on resume

// DOM helper
function el(tag, props = {}, ...children) {
  const e = document.createElement(tag);
  Object.assign(e, props);
  for (const c of children) {
    if (typeof c === 'string') e.appendChild(document.createTextNode(c));
    else if (c) e.appendChild(c);
  }
  return e;
}
function badge(text, title='') { return el('span', { className: 'badge', title }, text); }
function statusPill(state) {
  const cl = 'pill ' + (state==='active'?'ok': state==='idle'?'warn':'mutedpill');
  return el('span', { className: cl }, state || 'unknown');
}
function placeholder(text) { return el('div', { className: 'placeholder' }, text); }

// small helper for building elements with attributes & text
function elK(tag, attrs = {}, ...kids) {
  const n = document.createElement(tag);
  Object.entries(attrs).forEach(([k, v]) => {
    if (k === 'className') n.className = v;
    else if (k === 'dataset') Object.assign(n.dataset, v);
    else if (k in n) n[k] = v;
    else n.setAttribute(k, v);
  });
  for (const k of kids) n.appendChild(typeof k === 'string' ? document.createTextNode(k) : k);
  return n;
}

// DDS helpers (per-robot drawer)
// Treat any topic whose *path segments* include "dds" as a DDS item.
// Agnostic to vendor and to the leading namespace ("robot", "rdash", etc.).
// Configurable: add other markers if you want to bucket similar infra topics.
const DDS_MARKERS = ['dds'];
function isDdsSensorName(s) {
  if (!s) return false;
  const n = String(s).trim().replace(/^\/+/, '');
  const segs = n.split(/[\/]+/).map(x => x.toLowerCase());
  return DDS_MARKERS.some(m => segs.includes(m.toLowerCase()));
}

let ddsDrawerEl = null;
let ddsHandleEl = null;

function destroyDdsDrawer() {
  if (ddsHandleEl && ddsHandleEl.parentNode) ddsHandleEl.parentNode.removeChild(ddsHandleEl);
  if (ddsDrawerEl && ddsDrawerEl.parentNode) ddsDrawerEl.parentNode.removeChild(ddsDrawerEl);
  ddsHandleEl = null;
  ddsDrawerEl = null;
}

function attachDdsDrawerTo(detailEl) {
  // safety
  destroyDdsDrawer();

  // Drawer
  ddsDrawerEl = document.createElement('div');
  ddsDrawerEl.className = 'dds-drawer';
  ddsDrawerEl.setAttribute('aria-hidden', 'true');

  const header = el('div', { className: 'dds-header' },
    el('span', { className: 'dds-title' }, 'DDS'),
    el('span', { className: 'spacer' }, ''),
    el('button', { className: 'btn secondary', onclick: () => {
      ddsDrawerEl.classList.remove('open');
      ddsDrawerEl.setAttribute('aria-hidden', 'true');
      // resize active charts for crisp layout
      setTimeout(() => {
        for (const entry of chartsBySensor.values()) {
          try { entry?.ec?.resize({ animation: false }); } catch (_){}
        }
      }, 50);
    } }, 'Close')
  );
  const content = el('div', { className: 'dds-content' });
  ddsDrawerEl.appendChild(header);
  ddsDrawerEl.appendChild(content);

  // Handle (always shown on robot page)
  ddsHandleEl = document.createElement('button');
  ddsHandleEl.className = 'dds-drawer-handle';
  ddsHandleEl.title = 'DDS';
  ddsHandleEl.textContent = 'DDS (0)';
  ddsHandleEl.onclick = () => {
    const open = !ddsDrawerEl.classList.contains('open');
    ddsDrawerEl.classList.toggle('open', open);
    ddsDrawerEl.setAttribute('aria-hidden', String(!open));
    setTimeout(() => {
      for (const entry of chartsBySensor.values()) {
        try { entry?.ec?.resize({ animation: false }); } catch (_){}
      }
    }, 50);
  };

  // Attach to the page (robot detail)
  // We append the drawer first, then the handle so CSS sibling selector keeps z-order correct.
  document.body.appendChild(ddsDrawerEl);
  document.body.appendChild(ddsHandleEl);

  // start placeholder
  setDdsPlaceholder();
}

function setDdsPlaceholder() {
  if (!ddsDrawerEl) return;
  const wrap = ddsDrawerEl.querySelector('.dds-content');
  if (!wrap) return;
  wrap.innerHTML = '';
  wrap.appendChild(el('div', { className: 'placeholder', style: 'margin:8px 0' },
    'No DDS streams yet. You can bridge DDS→ROS 2 and publish under a dds/… namespace to group them here.'));
  if (ddsHandleEl) ddsHandleEl.textContent = 'DDS (0)';
}


// Disposer helper 
function disposeChartFor(sensorKey) {
  const entry = chartsBySensor.get(sensorKey);
  if (entry && entry.ec) {
    try { window.removeEventListener('resize', entry._resize); } catch (_) {}
    try { entry.ec.dispose(); } catch (_) {}
  }
  chartsBySensor.delete(sensorKey);
}

// HTTP helper
async function fetchJSON(url) {
  const r = await fetch(url, { headers: getHeaders(), credentials: 'same-origin', cache: 'no-store' });
  if (r.status === 401) { showGate(); throw new Error('unauthorized'); }
  if (!r.ok) throw new Error('http ' + r.status);
  return await r.json();
}

//  Series helpers (stable IDs)
function ensureSeriesIndex(entry, metric, labelIfCreate) {
  const ec = entry.ec;
  if (!ec) return -1;

  // Build/reuse stable ID for this metric
  let sid = entry.seriesIdByMetric.get(metric);
  if (!sid) {
    sid = `${entry.sensorKey}|${metric}`;
    entry.seriesIdByMetric.set(metric, sid);
  }

  // Find the series index by id
  let opt = ec.getOption();
  let idx = (opt.series || []).findIndex(s => s && s.id === sid);

  // If missing, create the series empty then find again
  if (idx === -1) {
    ec.setOption({
      series: [{
        id: sid,
        name: labelIfCreate || metric,
        type: 'line',
        showSymbol: false,
        sampling: 'lttb',
        data: []
      }]
    });
    opt = ec.getOption();
    idx = (opt.series || []).findIndex(s => s && s.id === sid);
  }
  return idx;
}

// Chart helpers 
function buildChart(canvas, sensorKey, hist, unitsMap, robot, sensor) {
  // Destroy any previous chart
  if (chartsBySensor.has(sensorKey)) {
    const prev = chartsBySensor.get(sensorKey);
    if (prev.ec) prev.ec.dispose();
    chartsBySensor.delete(sensorKey);
  }

  // Ensure wrapper and a crisp DIV container (not canvas)
  let wrap = canvas.closest('.chart-wrap');
  if (!wrap) {
    wrap = document.createElement('div');
    wrap.className = 'chart-wrap';
    canvas.replaceWith(wrap);
  } else {
    wrap.innerHTML = ''; // clean
  }

  // Toolbar (Pause / Delete)
  const toolbar = document.createElement('div');
  toolbar.className = 'chart-toolbar';
  toolbar.dataset.sensorKey = sensorKey;

  const pauseBtn = document.createElement('button');
  pauseBtn.className = 'btn secondary';
  pauseBtn.textContent = 'Pause';
  pauseBtn.title = 'Pause/resume live updates';

  const delBtn = document.createElement('button');
  delBtn.className = 'btn danger';
  delBtn.textContent = 'Delete Data';
  delBtn.title = 'Clear data for this sensor';

  toolbar.appendChild(pauseBtn);
  toolbar.appendChild(delBtn);

  // Plot container (DIV), set explicit size for crisp axis
  const elc = document.createElement('div');
  elc.style.width = '100%';
  elc.style.height = '320px'; // ensure x-axis has room & stays visible

  wrap.appendChild(toolbar);
  wrap.appendChild(elc);

  // Init ECharts (crisp)
  const ec = echarts.init(elc, null, {
    renderer: 'canvas',
    devicePixelRatio: Math.max(window.devicePixelRatio || 1, 1)
  });

  // Build initial series from history - use STABLE IDs
  const keys = Object.keys(hist || {}).sort();
  const series = [];
  const seriesIdByMetric = new Map(); // metric - seriesId

  for (const k of keys) {
    const pts = (hist[k] || []).map(([t, v]) => [t * 1000, v]); // ms
    const name = unitsMap && unitsMap[k] ? `${k} (${unitsMap[k]})` : k;
    const sid  = `${sensorKey}|${k}`;
    seriesIdByMetric.set(k, sid);
    series.push({
      id: sid,          // STABLE ID
      name,
      type: 'line',
      showSymbol: false,
      smooth: false,
      sampling: 'lttb',
      data: pts,
      progressive: 2000,
      progressiveThreshold: 4000,
    });
  }

  const WINDOW_SEC = 10, LEAD_SEC = 6;
  const now = Date.now();
  const min0 = now - (WINDOW_SEC + LEAD_SEC) * 1000;
  const max0 = now + LEAD_SEC * 1000;

  ec.setOption({
    animation: false,
    tooltip: {
      trigger: 'axis',
      // triggerOn: 'click', // kinda jittery so leaving it on default (hover)
      confine: false,
      axisPointer: { type: 'line' },
      valueFormatter: (v) => (Number.isFinite(v) ? v.toString() : '')
    },
    legend: { type: 'scroll' },
    grid: { left: 48, right: 16, top: 24, bottom: 28 },
    xAxis: {
      type: 'time',
      min: min0,
      max: max0,
      axisLabel: { formatter: (val) => fmtClock(val) }
    },
    yAxis: { type: 'value', scale: true },
    dataZoom: [
      // Hold Shift to zoom/pan so normal scrolling doesn’t interfere
      { type: 'inside', zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', moveOnMouseWheel: 'shift' },
      { type: 'slider', show: false }
    ],
    series
  });

  // Track “pinned” (user-zoomed) mode so we don’t auto-follow
  let pinned = false;
  ec.on('datazoom', () => { pinned = true; });

  // Double-click = reset zoom & resume follow (blank chart fix when reset)
  elc.addEventListener('dblclick', () => {
    pinned = false;
    ec.dispatchAction({ type: 'dataZoom', start: 0, end: 100 }); // reset zoom window
    const n = Date.now();
    ec.setOption({ xAxis: { min: n - (WINDOW_SEC + LEAD_SEC) * 1000, max: n + LEAD_SEC * 1000 } });
  });

  // Wire toolbar
  pauseBtn.addEventListener('click', async () => {
    const entry = chartsBySensor.get(sensorKey);
    if (!entry) return;

    entry.paused = !entry.paused;
    pauseBtn.textContent = entry.paused ? 'Resume' : 'Pause';

    if (entry.paused) {
      entry.pauseStarted = Date.now();
      entry.bufferByMetric.clear();
      return;
    }

    // RESUME:
    const pausedFor = Date.now() - (entry.pauseStarted || Date.now());

    if (pausedFor > REFILL_THRESHOLD_MS) {
      // Long pause: refetch latest history instead of replaying buffered points
      try {
        const hist2 = await fetchJSON(`/api/history/${encodeURIComponent(robot)}/${encodeURIComponent(sensor)}`);
        const keys2 = Object.keys(hist2 || {}).sort();

        // Rebuild all series fresh with stable IDs
        entry.seriesIdByMetric = new Map();
        const sers = [];
        for (const k of keys2) {
          const pts = (hist2[k] || []).map(([t, v]) => [t * 1000, v]);
          const sid = `${entry.sensorKey}|${k}`;
          entry.seriesIdByMetric.set(k, sid);
          sers.push({
            id: sid, name: k, type: 'line', showSymbol: false, sampling: 'lttb', data: pts
          });
        }

        // Reset series & x window (keep other options)
        const n = Date.now();
        ec.setOption({
          series: sers,
          xAxis: { type: 'time', min: n - (entry.windowSec + entry.leadSec) * 1000, max: n + entry.leadSec * 1000,
                   axisLabel: { formatter: (val) => fmtClock(val) } }
        }, { replaceMerge: ['series'] });

      } catch (e) {
        console.error('resume/history refill failed', e);
      }
    } else {
      // Short pause: flush buffered points in ONE batch per metric
      for (const [metric, arr] of entry.bufferByMetric.entries()) {
        if (!arr.length) continue;
        const idx = ensureSeriesIndex(entry, metric, metric);
        if (idx === -1) continue;
        try {
          ec.appendData({ seriesIndex: idx, data: arr });
        } catch (e) {
          // one-shot retry in case of race
          const idx2 = ensureSeriesIndex(entry, metric, metric);
          if (idx2 !== -1) {
            try { ec.appendData({ seriesIndex: idx2, data: arr }); } catch (_) {}
          }
        }
      }
    }

    // Clear buffer either way
    entry.bufferByMetric.clear();

    // Nudge x-axis to now
    const n = Date.now();
    ec.setOption({ xAxis: { min: n - (entry.windowSec + entry.leadSec) * 1000, max: n + entry.leadSec * 1000 } });
  });

  delBtn.addEventListener('click', async () => {
    const entry = chartsBySensor.get(sensorKey);
    if (entry) {
      // --- reset local state (no global side effects) ---
      entry.bufferByMetric?.clear?.();
      entry.pauseStarted = 0;
      entry.dropped = 0;
      entry.capped = false;

      // If it was paused, flip back to running AND update the button label
      if (entry.paused) {
        entry.paused = false;
        if (pauseBtn) pauseBtn.textContent = 'Pause';
      }

      // Follow live again after delete
      entry.pinned = false;

      // Reset ID map so fresh metrics recreate safely
      entry.seriesIdByMetric = new Map();

      // Keep options (grid/legend/tooltip), just blank series & reset x window
      const n = Date.now();
      const opt = entry.ec.getOption();
      entry.ec.setOption({
        series: [],  // safely remove; rAF path will add back per-metric series
        xAxis: [{
          ...(opt.xAxis && opt.xAxis[0] ? opt.xAxis[0] : { type: 'time' }),
          min: n - (entry.windowSec + entry.leadSec) * 1000,
          max: n + entry.leadSec * 1000,
          axisLabel: { formatter: (val) => fmtClock(val) }
        }]
      }, { replaceMerge: ['series'] });
    }

    // Fire and forget to the server
    try {
      const rname = robot ?? sensorKey.split('/')[0];
      const sname = sensor ?? sensorKey.split('/').slice(1).join('/');
      await fetch(`/api/delete_series/${encodeURIComponent(rname)}/${encodeURIComponent(sname)}`, {
        method: 'POST', headers: getHeaders()
      });
    } catch (_) {}
  });

  // Resize crisp on window changes
  const resize = () => ec.resize({ animation: false });
  window.addEventListener('resize', resize);

  chartsBySensor.set(sensorKey, {
    ec,
    // legacy field kept but no longer relied upon for indexes
    datasets: new Map(),
    seriesIdByMetric,          // NEW: metric -> stable series id
    paused: false,
    // NEW:
    pauseStarted: 0,
    bufferByMetric: new Map(), // metric -> [[ms, val], ...]
    windowSec: WINDOW_SEC,
    leadSec: LEAD_SEC,
    pinned,
    _resize: resize,
    sensorKey                   // NEW: used for building stable IDs
  });
}

function updateAxesWindow(entry, latestMs) {
  if (!entry || entry.paused) return;
  const c = entry.chart;
  const now = latestMs || (Date.now());
  const center = (latestMs || Date.now()) + entry.leadSec * 1000;
  const min = center - entry.windowSec * 1000;
  const max = center + entry.leadSec * 1000;
  c.options.scales.x.min = min;
  c.options.scales.x.max = max;
}

function applyBufferedUpdates() {
  pendingUpdates.forEach((buf, sensorKey) => {
    const entry = chartsBySensor.get(sensorKey);
    if (!entry) return;

    const { ec, paused, pinned, windowSec, leadSec } = entry;
    if (!ec) return;

    // Buffer while paused (per-metric arrays + cap/decimate handled in WS handler)
    if (paused) {
      // Nothing: WS handler already stuffed bufferByMetric while paused
      return;
    }

    const whenMs = buf.t * 1000;

    // Append per metric using STABLE IDs
    for (const k in buf.data) {
      const v = buf.data[k];
      if (!Number.isFinite(v)) continue;

      const idx = ensureSeriesIndex(entry, k, k);
      if (idx === -1) continue;

      try {
        ec.appendData({ seriesIndex: idx, data: [[whenMs, v]] });
      } catch (e) {
        // recreate & retry once if a race occurred
        const idx2 = ensureSeriesIndex(entry, k, k);
        if (idx2 !== -1) {
          try { ec.appendData({ seriesIndex: idx2, data: [[whenMs, v]] }); } catch (_) {}
        }
      }
    }

    // Auto-follow latest unless user pinned by zooming
    if (!pinned) {
      ec.setOption({
        xAxis: { min: whenMs - (windowSec + leadSec) * 1000, max: whenMs + leadSec * 1000 }
      }, { lazyUpdate: true });
    }
  });

  pendingUpdates.clear();
  rafScheduled = false;
}

// Overview
async function loadRobotsInto(container) {
  try {
    const data = await fetchJSON('/api/robots');
    hideGate();
    setStatus(ws && ws.connected ? 'Connected' : 'Idle');

    container.innerHTML = '';
    const entries = Object.entries(data.robots || {});
    if (entries.length === 0) {
      container.appendChild(el('div', { className: 'muted' }, 'Waiting for robots to push data…'));
      return;
    }
    for (const [name, meta] of entries) {
      const card = el('div', { className: 'card robot-card' });
      card.appendChild(el('div', { className: 'robot-title' }, name));

      const status = (data.status || {})[name] || 'disconnected';
      card.appendChild(el('div', { style: 'margin:6px 0' }, statusPill(status)));

      const info = el('div', { className: 'muted', style: 'margin:6px 0' }, `${meta.sensors.length} sensors`);
      card.appendChild(info);

      const pills = el('div');
      if (meta.cameras.length) pills.appendChild(el('span', { className: 'pill' }, 'Camera'));
      // audio tiles not shown in UI (API kept)
      // optional: show log presence
      if (meta.texts && meta.texts.length) pills.appendChild(el('span', { className: 'pill' }, 'Text'));
      card.appendChild(pills);

      card.onclick = () => navigateRobot(name);
      container.appendChild(card);
    }
  } catch (e) {
    console.error(e);
  }
}
function renderOverview() {
  // ensure DDS drawer is only on robot page
  destroyDdsDrawer();

  currentRobot = null;
  const detail = document.getElementById('detail');
  detail.style.display = 'none';
  const robots = document.getElementById('robots');
  robots.innerHTML = '';
  loadRobotsInto(robots);
}
function navigateOverview() { renderOverview(); }

// TF tree render
function renderTfTree(container, edges) {
  if (!edges || !edges.length) {
    container.appendChild(placeholder('No TF frames'));
    return;
  }
  const map = {};
  edges.forEach(([p, c]) => { (map[p] ||= []).push(c); });
  const childrenSet = new Set(edges.map(([_, c]) => c));
  const roots = Object.keys(map).filter(p => !childrenSet.has(p));
  const ul = el('ul', { className: 'tf-tree' });
  function addNode(parent, name) {
    const li = el('li', {}, name);
    const kids = map[name] || [];
    if (kids.length) {
      const sub = el('ul', {});
      kids.forEach(k => addNode(sub, k));
      li.appendChild(sub);
    }
    parent.appendChild(li);
  }
  (roots.length ? roots : Object.keys(map)).slice(0, 3).forEach(r => addNode(ul, r));
  container.appendChild(ul);
}

// Text log helpers
function appendLogLine(sensorKey, ts, text, level) {
  const entry = logsBySensor.get(sensorKey);
  if (!entry || !entry.containerEl) return;

  const line = el('div', { className: `log-line ${level ? ('lvl-'+String(level).toLowerCase()) : ''}` });
  const tsSpan = el('span', { className: 'log-ts' }, fmtClock(ts * 1000));
  const txtSpan = el('span', { className: 'log-txt' }, text);

  line.appendChild(tsSpan);
  line.appendChild(document.createTextNode(' '));
  line.appendChild(txtSpan);
  entry.containerEl.appendChild(line);

  // keep some limit in DOM (near server default)
  entry.lines = (entry.lines || 0) + 1;
  if (entry.lines > 600) {
    while (entry.containerEl.childElementCount > 500) {
      entry.containerEl.removeChild(entry.containerEl.firstChild);
    }
    entry.lines = entry.containerEl.childElementCount;
  }

  if (!entry.paused && entry.follow) {
    entry.containerEl.parentElement.scrollTop = entry.containerEl.parentElement.scrollHeight;
  }
}

// Robot page
async function navigateRobot(name) {
  currentRobot = name;

  const robots = document.getElementById('robots'); robots.innerHTML = '';
  const detail = document.getElementById('detail'); detail.innerHTML = ''; detail.style.display = '';

  const navBar = el('div', { className: 'breadcrumb' });
  const backBtn = el('button', { className: 'btn secondary' }, '← Back');
  backBtn.onclick = () => navigateOverview();
  navBar.append(backBtn, el('span', { className: 'crumb' }, ' / '), el('span', { className: 'crumb' }, name), el('span', { className: 'crumb muted' }, ' / sensors'));
  detail.appendChild(navBar);
  detail.appendChild(el('h2', {}, 'Robot: ', name));

  // Attach per-robot DDS drawer (persistent on robot detail page)
  attachDdsDrawerTo(detail);

  // Meta
  let metaSensors = [], metaCams = [], metaTexts = [];
  let types = {};
  let statuses = {};
  try {
    const robotsData = await fetchJSON('/api/robots');
    const meta = robotsData.robots?.[name] || { sensors: [], cameras: [], audios: [], texts: [] };
    metaSensors = meta.sensors || [];
    metaCams    = meta.cameras || [];
    metaTexts   = meta.texts || [];
    types       = await fetchJSON(`/api/types/${encodeURIComponent(name)}`);
    const st    = await fetchJSON(`/api/status/${encodeURIComponent(name)}`);
    statuses    = st || {};

    // 1) Split DDS vs non-DDS ONCE here (these four are in scope for the rest of the function)
    const ddsSensors     = metaSensors.filter(isDdsSensorName);
    const ddsTexts       = metaTexts.filter(isDdsSensorName);
    var   regularSensors = metaSensors.filter(s => !isDdsSensorName(s));  // define before use
    var   regularTexts   = metaTexts.filter(s => !isDdsSensorName(s));    // define before use

    // Seed the known DDS sets to avoid rebuild at data rate tick
    const idx = _ensureDdsIndex(name);
    idx.sensors = new Set(ddsSensors);
    idx.texts   = new Set(ddsTexts);

    console.debug('DDS split', {
      allSensors: metaSensors,
      ddsSensors,
      regularSensors,
      allTexts: metaTexts,
      ddsTexts,
      regularTexts
    });

    // 2) Build the DDS drawer content
    await rebuildDdsDrawerFor(name, ddsSensors, ddsTexts, types, statuses);

    // (no rendering here; the page stacks will use regularSensors/regularTexts below)
  } catch (e) {
    console.error(e);
    // fallback if the API failed so the page still renders something
    var regularSensors = metaSensors || [];
    var regularTexts   = metaTexts || [];
  }

  // TF preview
  try {
    const tf = await fetchJSON(`/api/tf/${encodeURIComponent(name)}`);
    const tfWrap = el('div', { className: 'panel', style: 'margin-bottom:12px' });
    tfWrap.appendChild(el('div', { className: 'muted' }, 'TF frames'));
    renderTfTree(tfWrap, (tf && tf.edges) || []);
    detail.appendChild(tfWrap);
  } catch (e) { /* ignore */ }

  // Cameras
  if (metaCams.length) {
    const camsStack = el('div', { className: 'sensor-stack' });
    for (const cam of metaCams) {
      const acc = el('details', { className: 'accordion' });
      const typBadge = types[cam] ? badge(types[cam].split('/').pop(), types[cam]) : null;
      const state = (statuses[cam] && statuses[cam].status) || 'disconnected';
      const sum = el('summary', { className: 'accordion-summary' }, cam, ' ', typBadge || '', ' ', statusPill(state));
      const body = el('div', { className: 'accordion-body' });

      const panel = el('div', { className: 'panel' });
      panel.appendChild(el('div', { className: 'muted' }, cam));
      const holder = el('div', { className: 'media-holder' });
      const msg = el('div', { className: 'placeholder', style: 'display:none' }, 'No video available');
      const img = el('img', { className: 'video', src: '', style: 'display:none' });
      holder.append(msg, img);
      panel.appendChild(holder);
      body.appendChild(panel);

      let loaded = false;
      acc.addEventListener('toggle', async () => {
        if (!acc.open || loaded) return;
        try {
          const head = await fetch(`/video/${encodeURIComponent(name)}/${encodeURIComponent(cam)}?wait=0`, { method: 'GET', headers: getHeaders(), cache: 'no-store' });
          if (!head.ok) { msg.style.display = ''; img.style.display = 'none'; loaded = true; return; }
        } catch(_) {}
        img.onerror = () => { msg.style.display = ''; img.style.display = 'none'; };
        img.onload = () => { img.style.display = ''; msg.style.display = 'none'; };
        const tok = getToken(); const qs = tok ? `?token=${encodeURIComponent(tok)}` : '';
        img.src = `/video/${encodeURIComponent(name)}/${encodeURIComponent(cam)}${qs}`;
        loaded = true;
      });

      acc.append(sum, body);
      camsStack.appendChild(acc);
    }
    detail.appendChild(camsStack);
  }

  // Numeric sensors
  const stack = el('div', { className: 'sensor-stack' });
  for (const s of regularSensors.filter(s => !isDdsSensorName(s))) {
    const acc = el('details', { className: 'accordion' });
    const typBadge = types[s] ? badge(types[s].split('/').pop(), types[s]) : null;
    const state = (statuses[s] && statuses[s].status) || 'disconnected';
    const sum = el('summary', { className: 'accordion-summary' }, s, ' ', typBadge || '', ' ', statusPill(state));
    const body = el('div', { className: 'accordion-body' });

    const panel = el('div', { className: 'panel' });
    panel.appendChild(el('div', { className: 'muted' }, s));

    // NOTE: we don't keep a persistent canvas; we create a fresh one on open.
    body.appendChild(panel);
    acc.append(sum, body);
    stack.appendChild(acc);

    const sensorKey = `${name}/${s}`;

    acc.addEventListener('toggle', async () => {
      // On close: dispose & clean DOM
      if (!acc.open) {
        disposeChartFor(sensorKey);
        const oldWrap = panel.querySelector('.chart-wrap');
        if (oldWrap) oldWrap.remove();
        return;
      }

      // On open: clean any old wrapper and mount a fresh canvas before calling buildChart
      const prevWrap = panel.querySelector('.chart-wrap');
      if (prevWrap) prevWrap.remove();

      const canvas = document.createElement('canvas');
      panel.appendChild(canvas);

      try {
        const [hist, meta] = await Promise.all([
          fetchJSON(`/api/history/${encodeURIComponent(name)}/${encodeURIComponent(s)}`),
          fetchJSON(`/api/meta/${encodeURIComponent(name)}/${encodeURIComponent(s)}`)
        ]);

        buildChart(canvas, sensorKey, hist, (meta && meta.units) || {}, name, s);
        ensureSocket();

        // After the details expands, give layout a tick then ensure a crisp resize.
        setTimeout(() => {
          const entry = chartsBySensor.get(sensorKey);
          if (entry && entry.ec) entry.ec.resize({ animation: false });
        }, 0);
      } catch (e) {
        console.error('chart init failed', e);
      }
    });
  }
  detail.appendChild(stack);

  // Text streams (force render at bottom)
  if (regularTexts.length) {
    const textStack = el('div', { className: 'sensor-stack' });
    for (const tname of regularTexts.filter(s => !isDdsSensorName(s))) {
      const acc = el('details', { className: 'accordion' });
      const typBadge = types[tname] ? badge(types[tname].split('/').pop(), types[tname]) : null;
      const state = (statuses[tname] && statuses[tname].status) || 'disconnected';
      const sum = el('summary', { className: 'accordion-summary' }, tname, ' ', typBadge || '', ' ', statusPill(state));
      const body = el('div', { className: 'accordion-body' });

      const panel = el('div', { className: 'panel' });
      panel.appendChild(el('div', { className: 'muted' }, tname));

      // Toolbar
      const tools = el('div', { className: 'chart-toolbar' });
      const pauseBtn = el('button', { className: 'btn secondary' }, 'Pause');
      const clearBtn = el('button', { className: 'btn danger' }, 'Delete Data');
      tools.appendChild(pauseBtn);
      tools.appendChild(clearBtn);
      panel.appendChild(tools);

      // Log viewport
      const wrap = el('div', { className: 'log-wrap' });
      const lines = el('div', { className: 'log-lines' });
      wrap.appendChild(lines);
      panel.appendChild(wrap);

      body.appendChild(panel);
      acc.append(sum, body);
      textStack.appendChild(acc);

      const sensorKey = `${name}/${tname}`;

      acc.addEventListener('toggle', async () => {
        if (!acc.open) return;

        // init entry
        if (!logsBySensor.has(sensorKey)) {
          logsBySensor.set(sensorKey, { containerEl: lines, paused: false, follow: true, lines: 0 });
        } else {
          const e = logsBySensor.get(sensorKey);
          e.containerEl = lines;
        }

        // load history
        try {
          const hist = await fetchJSON(`/api/text_history/${encodeURIComponent(name)}/${encodeURIComponent(tname)}`);
          const e = logsBySensor.get(sensorKey);
          if (e?.containerEl) {
            e.containerEl.innerHTML = '';
            e.lines = 0;
            for (const row of hist) {
              appendLogLine(sensorKey, row.t, row.text, row.level);
            }
          }
          ensureSocket();
        } catch (err) {
          console.error('text history failed', err);
        }
      });

      // Wire toolbar buttons
      pauseBtn.addEventListener('click', () => {
        const entry = logsBySensor.get(sensorKey);
        if (!entry) return;
        entry.paused = !entry.paused;
        pauseBtn.textContent = entry.paused ? 'Resume' : 'Pause';
        if (!entry.paused) entry.follow = true;
      });

      clearBtn.addEventListener('click', async () => {
        const entry = logsBySensor.get(sensorKey);
        if (entry?.containerEl) {
          entry.containerEl.innerHTML = '';
          entry.lines = 0;
        }
        try {
          await fetch(`/api/delete_text/${encodeURIComponent(name)}/${encodeURIComponent(tname)}`, {
            method: 'POST',
            headers: getHeaders()
          });
        } catch (_) {}
      });
    }
    detail.appendChild(textStack); // appended last
  }
}

// Rebuild drawer only for each specific robot (call from navigateRobot)
async function rebuildDdsDrawerFor(robot, ddsSensors, ddsTexts, types, statuses) {
  if (!ddsDrawerEl) return; // drawer created by attachDdsDrawerTo(detail)
  const wrap = ddsDrawerEl.querySelector('.dds-content');
  //wrap.innerHTML = '';

  // Preserve which accordions were open (keyed by sensor name) ---new
  const openSet = new Set(
    Array.from(wrap.querySelectorAll('details.accordion[open]'))
      .map(d => d.getAttribute('data-sensor'))
      .filter(Boolean)
  );
  wrap.innerHTML = '';
  // ---new

  const count = (ddsSensors?.length || 0) + (ddsTexts?.length || 0);
  if (ddsHandleEl) ddsHandleEl.textContent = `DDS (${count})`;

  if (!count) { setDdsPlaceholder(); return; }
  ddsHandleEl.textContent = `DDS (${count})`;

  // Metrics group
  if (ddsSensors?.length) {
    wrap.appendChild(el('div', { className: 'muted', style: 'margin:8px 0 4px' }, 'Metrics'));
    for (const s of ddsSensors) {
      const acc = el('details', { className: 'accordion' });
      acc.setAttribute('data-sensor', s);
      const typBadge = types[s] ? badge(types[s].split('/').pop(), types[s]) : null;
      const state = (statuses[s] && statuses[s].status) || 'disconnected';
      const sum = el('summary', { className: 'accordion-summary' }, s, ' ', typBadge || '', ' ', statusPill(state));
      const body = el('div', { className: 'accordion-body' });
      const panel = el('div', { className: 'panel' });
      panel.appendChild(el('div', { className: 'muted' }, s));
      body.appendChild(panel);
      acc.append(sum, body);
      wrap.appendChild(acc);

      // Restore previously-open state ---new
      if (openSet.has(s)) acc.open = true;
      // ---new

      const sensorKey = `${robot}/${s}`;
      acc.addEventListener('toggle', async () => {
        if (!acc.open) {
          disposeChartFor(sensorKey);
          const oldWrap = panel.querySelector('.chart-wrap');
          if (oldWrap) oldWrap.remove();
          return;
        }
        const prevWrap = panel.querySelector('.chart-wrap');
        if (prevWrap) prevWrap.remove();
        const canvas = document.createElement('canvas');
        panel.appendChild(canvas);
        try {
          const [hist, meta] = await Promise.all([
            fetchJSON(`/api/history/${encodeURIComponent(robot)}/${encodeURIComponent(s)}`),
            fetchJSON(`/api/meta/${encodeURIComponent(robot)}/${encodeURIComponent(s)}`)
          ]);
          buildChart(canvas, sensorKey, hist, (meta && meta.units) || {}, robot, s);
          ensureSocket();
          setTimeout(() => {
            const entry = chartsBySensor.get(sensorKey);
            if (entry && entry.ec) entry.ec.resize({ animation: false });
          }, 0);
        } catch (e) { console.error('DDS chart init failed', e); }
      });
    }
  }

  // Logs group
  if (ddsTexts?.length) {
    wrap.appendChild(el('div', { className: 'muted', style: 'margin:12px 0 4px' }, 'Logs'));
    for (const tname of ddsTexts) {
      const acc = el('details', { className: 'accordion' });
      acc.setAttribute('data-sensor', tname);

      const typBadge = types[tname] ? badge(types[tname].split('/').pop(), types[tname]) : null;
      const state = (statuses[tname] && statuses[tname].status) || 'disconnected';
      const sum = el('summary', { className: 'accordion-summary' }, tname, ' ', typBadge || '', ' ', statusPill(state));
      const body = el('div', { className: 'accordion-body' });

      const panel = el('div', { className: 'panel' });
      panel.appendChild(el('div', { className: 'muted' }, tname));

      const tools = el('div', { className: 'chart-toolbar' });
      const pauseBtn = el('button', { className: 'btn secondary' }, 'Pause');
      const clearBtn = el('button', { className: 'btn danger' }, 'Delete Data');
      tools.appendChild(pauseBtn); tools.appendChild(clearBtn);
      panel.appendChild(tools);

      const wrapLog = el('div', { className: 'log-wrap' });
      const lines = el('div', { className: 'log-lines' });
      wrapLog.appendChild(lines);
      panel.appendChild(wrapLog);

      body.appendChild(panel);
      acc.append(sum, body);
      wrap.appendChild(acc);

      // Restore previously-open state 
      if (openSet.has(tname)) acc.open = true; // ---new

      const sensorKey = `${robot}/${tname}`;

      acc.addEventListener('toggle', async () => {
        if (!acc.open) return;
        if (!logsBySensor.has(sensorKey)) {
          logsBySensor.set(sensorKey, { containerEl: lines, paused: false, follow: true, lines: 0 });
        } else {
          const e = logsBySensor.get(sensorKey);
          e.containerEl = lines;
        }
        try {
          const hist = await fetchJSON(`/api/text_history/${encodeURIComponent(robot)}/${encodeURIComponent(tname)}`);
          const e = logsBySensor.get(sensorKey);
          if (e?.containerEl) {
            e.containerEl.innerHTML = '';
            e.lines = 0;
            for (const row of hist) appendLogLine(sensorKey, row.t, row.text, row.level);
          }
          ensureSocket();
        } catch (err) { console.error('DDS text history failed', err); }
      });

      pauseBtn.addEventListener('click', () => {
        const entry = logsBySensor.get(sensorKey);
        if (!entry) return;
        entry.paused = !entry.paused;
        pauseBtn.textContent = entry.paused ? 'Resume' : 'Pause';
        if (!entry.paused) entry.follow = true;
      });

      clearBtn.addEventListener('click', async () => {
        const entry = logsBySensor.get(sensorKey);
        if (entry?.containerEl) {
          entry.containerEl.innerHTML = '';
          entry.lines = 0;
        }
        try {
          await fetch(`/api/delete_text/${encodeURIComponent(robot)}/${encodeURIComponent(tname)}`, {
            method: 'POST', headers: getHeaders()
          });
        } catch (_) {}
      });
    }
  }
}

// WebSocket 
function ensureSocket(){
  const token = getToken();
  if (ws && ws.connected) return;
  setStatus('Connecting…');
  ws = io('/ws', { auth: { token }, transports: ['websocket'] }); // [.., 'polling'] makes it slow

  ws.on('connect', () => { setStatus('Connected'); hideGate(); });
  ws.on('disconnect', () => { setStatus('Idle'); });
  ws.on('connect_error', (err) => { console.warn('WS connect_error', err && err.message); setStatus('Unauthorized'); showGate(); });
  ws.on('sensor_data', (msg) => {
    const { robot, sensor, t, data } = msg;

    if (currentRobot === robot && isDdsSensorName(sensor)) {
      _ddsPrimed = true;
      const idx = _ensureDdsIndex(robot);
      if (!idx.sensors.has(sensor)) {
        idx.sensors.add(sensor);
        // Only rebuild if a *new* DDS metric appeared
        refreshDdsDrawerIfNeeded(robot).catch(()=>{});
      }
    }
    const sensorKey = `${robot}/${sensor}`; //keep?

    const entry = chartsBySensor.get(sensorKey);
    if (entry && entry.paused) {
      // Per-metric buffered arrays + decimation
      const m = entry.bufferByMetric;
      const tsMs = t * 1000;

      for (const k in data) {
        const v = data[k];
        if (!Number.isFinite(v)) continue;
        let arr = m.get(k);
        if (!arr) { arr = []; m.set(k, arr); }
        arr.push([tsMs, v]);

        // Hard cap + decimate (keep every 2nd when overflowing)
        if (arr.length > MAX_BUFFER_POINTS) {
          const thin = [];
          for (let i = 0; i < arr.length; i += 2) thin.push(arr[i]);
          m.set(k, thin);
        }
      }
      return;
    }

    // coalesce latest values until next rAF
    const prev = pendingUpdates.get(sensorKey) || { data: {}, t };
    prev.t = t;
    for (const k in data) prev.data[k] = data[k];
    pendingUpdates.set(sensorKey, prev);

    if (!rafScheduled) {
      rafScheduled = true;
      requestAnimationFrame(applyBufferedUpdates);
    }
  });

  // NEW: live text lines
   ws.on('text_data', (msg) => {
     const { robot, sensor, t, text, level } = msg;
    if (currentRobot === robot && isDdsSensorName(sensor)) {
      const idx = _ensureDdsIndex(robot);
      if (!idx.texts.has(sensor)) {
        idx.texts.add(sensor);
        // Only rebuild if a *new* DDS text stream appeared
        refreshDdsDrawerIfNeeded(robot).catch(()=>{});
      }
    }
     const sensorKey = `${robot}/${sensor}`;
     const entry = logsBySensor.get(sensorKey);
     if (!entry) return;
     if (entry.paused) return;
     appendLogLine(sensorKey, t, text, level);
   });

}

// Boot
document.addEventListener('DOMContentLoaded', async () => {
  try {
    const head = await fetch('/', { method: 'HEAD', cache: 'no-store' });
    const need = (head.headers.get('X-URDAF-Auth') || '').toLowerCase();
    const hasToken = !!getToken();
    if (need === 'required' && !hasToken) showGate();
  } catch(_) {}

  ensureSocket();
  navigateOverview();
  setInterval(() => { if (!currentRobot) navigateOverview(); }, 1500);
});

