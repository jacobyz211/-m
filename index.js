// SoundCloud + Deezer (SNIP-only) — Eclipse addon worker (Cloudflare Workers)
// Primary: SoundCloud search + streams
// Fallback: Deezer via ARL ONLY when SoundCloud marks track as 30s preview (policy SNIP)

import { Hono } from 'hono';
import { cors } from 'hono/cors';

const app = new Hono();
app.use('*', cors());

// ─── Constants ───────────────────────────────────────────────────────────────
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

const RATE_MAX          = 60;
const RATE_WINDOW_MS    = 60000;
const MAX_TOKENS_PER_IP = 10;

// In-memory caches (per isolate)
const TOKEN_CACHE = new Map();   // token -> entry
const TRACK_CACHE = new Map();   // scId  -> meta
const IP_BUCKETS  = new Map();   // ip    -> { count, resetAt }

let SHARED_CLIENT_ID = null;
let _scFetchPromise  = null;

const _inflight = new Map();     // dedupe streams

// ─── Small helpers ───────────────────────────────────────────────────────────
function cleanText(s) {
  return String(s || '').replace(/\s+/g, ' ').trim();
}

function artworkUrl(raw, fb) {
  const s = raw || fb || '';
  return s ? s.replace('-large', '-t500x500') : null;
}

function scYear(x) {
  return (x.release_date || x.created_at || '').slice(0, 4) || null;
}

function parseArtistTitle(track) {
  const raw  = cleanText(track && track.title);
  const meta = cleanText(
    (track &&
      track.publisher_metadata &&
      (track.publisher_metadata.artist ||
       track.publisher_metadata.writer_composer)) || ''
  );
  const up = cleanText(track && track.user && track.user.username);
  return { artist: meta || up, title: raw, rawTitle: raw, uploader: up };
}

function rememberTrack(t) {
  if (!t || !t.id) return;
  const m = parseArtistTitle(t);
  const isrc = (t.publisher_metadata && t.publisher_metadata.isrc) || t.isrc || null;
  TRACK_CACHE.set(String(t.id), {
    id:       String(t.id),
    artist:   m.artist,
    title:    m.title,
    rawTitle: m.rawTitle,
    uploader: m.uploader,
    isrc:     isrc || null
  });
}

function getBaseUrl(c) {
  const proto = c.req.header('x-forwarded-proto') || 'https';
  return proto + '://' + c.req.header('host');
}

function effectiveCid(entry, env) {
  return (entry && entry.clientId) ? entry.clientId : (env.SC_CLIENT_ID || SHARED_CLIENT_ID);
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function dedupeCall(key, fn) {
  if (_inflight.has(key)) return _inflight.get(key);
  const p = Promise.resolve().then(fn).finally(() => _inflight.delete(key));
  _inflight.set(key, p);
  return p;
}

// ─── Upstash Redis (HTTP) ────────────────────────────────────────────────────
async function redisCmd(env, ...args) {
  if (!env.REDIS_URL || !env.REDIS_TOKEN) return null;
  try {
    const r = await fetch(env.REDIS_URL + '/' + args.map(encodeURIComponent).join('/'), {
      headers: { Authorization: 'Bearer ' + env.REDIS_TOKEN }
    });
    const j = await r.json();
    return j.result !== undefined ? j.result : null;
  } catch { return null; }
}

async function redisGet(env, key) {
  return redisCmd(env, 'GET', key);
}

async function redisSet(env, key, value, ex) {
  if (ex) return redisCmd(env, 'SET', key, value, 'EX', String(ex));
  return redisCmd(env, 'SET', key, value);
}

// ─── Token store ─────────────────────────────────────────────────────────────
function generateToken() {
  const arr = new Uint8Array(14);
  crypto.getRandomValues(arr);
  return Array.from(arr).map(b => b.toString(16).padStart(2, '0')).join('');
}

async function redisSave(env, token, entry) {
  await redisSet(env, 'sc:token:' + token, JSON.stringify({
    clientId:   entry.clientId,
    oauthToken: entry.oauthToken || null,
    createdAt:  entry.createdAt,
    lastUsed:   entry.lastUsed,
    reqCount:   entry.reqCount
  }));
}

async function getTokenEntry(env, token) {
  if (TOKEN_CACHE.has(token)) return TOKEN_CACHE.get(token);

  const saved = await redisGet(env, 'sc:token:' + token);
  if (saved) {
    try {
      const d = JSON.parse(saved);
      const entry = { ...d, oauthToken: d.oauthToken || null, rateWin: [] };
      TOKEN_CACHE.set(token, entry);
      return entry;
    } catch {}
  }

  if (/^[a-f0-9]{28}$/.test(token)) {
    const fresh = {
      clientId:   null,
      oauthToken: null,
      createdAt:  Date.now(),
      lastUsed:   Date.now(),
      reqCount:   0,
      rateWin:    []
    };
    TOKEN_CACHE.set(token, fresh);
    return fresh;
  }
  return null;
}

function checkRateLimit(entry) {
  const now = Date.now();
  entry.rateWin = (entry.rateWin || []).filter(t => now - t < RATE_WINDOW_MS);
  if (entry.rateWin.length >= RATE_MAX) return false;
  entry.rateWin.push(now);
  entry.lastUsed = now;
  entry.reqCount = (entry.reqCount || 0) + 1;
  return true;
}

function getOrCreateIpBucket(ip) {
  const now = Date.now();
  let b = IP_BUCKETS.get(ip);
  if (!b || now > b.resetAt) {
    b = { count: 0, resetAt: now + 86400000 };
    IP_BUCKETS.set(ip, b);
  }
  return b;
}

// ─── HTTP helpers ────────────────────────────────────────────────────────────
async function httpGet(url, params, headers, timeout) {
  const u = new URL(url);
  if (params) Object.entries(params).forEach(([k, v]) => u.searchParams.set(k, v));
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeout || 15000);
  try {
    const r = await fetch(u.toString(), {
      headers: { 'User-Agent': UA, 'Accept': 'application/json', ...(headers || {}) },
      signal: ctrl.signal
    });
    clearTimeout(timer);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return r.json();
  } catch (e) {
    clearTimeout(timer);
    throw e;
  }
}

async function httpGetText(url, headers, timeout) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeout || 15000);
  try {
    const r = await fetch(url, {
      headers: { 'User-Agent': UA, 'Accept': 'text/html,*/*', ...(headers || {}) },
      signal: ctrl.signal
    });
    clearTimeout(timer);
    return r.ok ? r.text() : null;
  } catch {
    clearTimeout(timer);
    return null;
  }
}

// ─── SoundCloud client_id scraping ───────────────────────────────────────────
const ID_PATTERNS = [
  /client_id\s*[=:,]\s*["']([a-zA-Z0-9]{32})["']/,
  /"client_id"\s*:\s*"([a-zA-Z0-9]{32})"/,
  /"client_id","([a-zA-Z0-9]{32})"/,
  /client_id=([a-zA-Z0-9]{32})[&"' \s,)]/
];

function findId(text) {
  for (const p of ID_PATTERNS) {
    const m = text.match(p);
    if (m) return m[1];
  }
  return null;
}

async function tryExtract() {
  for (const pu of ['https://soundcloud.com', 'https://soundcloud.com/discover']) {
    const html = await httpGetText(pu);
    if (!html || html.length < 5000) continue;
    for (const m of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)) {
      const id = findId(m[1] || '');
      if (id) return id;
    }
  }
  return null;
}

async function fetchSharedClientId(env) {
  if (env.SC_CLIENT_ID) { SHARED_CLIENT_ID = env.SC_CLIENT_ID; return; }
  if (_scFetchPromise) return _scFetchPromise;
  _scFetchPromise = (async () => {
    const cached = await redisGet(env, 'sc:shared_client_id');
    if (cached) { SHARED_CLIENT_ID = cached; return; }
    const delays = [5000, 10000, 15000, 30000, 60000];
    let attempt = 0;
    while (attempt < 5) {
      attempt++;
      try {
        const id = await tryExtract();
        if (!id) throw new Error('not found');
        SHARED_CLIENT_ID = id;
        await redisSet(env, 'sc:shared_client_id', id, 18000);
        return;
      } catch {
        await sleep(delays[Math.min(attempt - 1, delays.length - 1)]);
      }
    }
  })().finally(() => { _scFetchPromise = null; });
  return _scFetchPromise;
}

async function scGet(cid, url, params, oauthToken) {
  if (!cid) throw new Error('No client_id');
  const hdrs = oauthToken ? { Authorization: 'OAuth ' + oauthToken } : {};
  return httpGet(url, { ...params, client_id: cid }, hdrs);
}

function scOAuth(entry) {
  return (entry && entry.oauthToken) ? entry.oauthToken : null;
}

// Resolve stub tracks -> full metadata
async function resolveStubs(cid, tracks, fbArt, oauthToken) {
  const stubs = tracks.filter(t => !t.title).map(t => t.id);
  const map   = {};
  for (let i = 0; i < stubs.length; i += 50) {
    const batch = stubs.slice(i, i + 50);
    try {
      const data = await scGet(cid, 'https://api-v2.soundcloud.com/tracks', { ids: batch.join(',') }, oauthToken);
      const arr = Array.isArray(data) ? data : data.collection ? data.collection : [];
      arr.forEach(t => { map[String(t.id)] = t; });
    } catch {}
  }
  return tracks.map(t => map[String(t.id)] || t).map(t => t.title ? t : { ...t, title: 'Unknown Track' });
}

// ─── Deezer direct API (ARL) ──────────────────────────────────────────────────
async function deezerApi(env, path, params) {
  const ARL = env.DEEZER_ARL;
  if (!ARL) throw new Error('No DEEZER_ARL configured');
  const u = new URL('https://api.deezer.com' + path);
  if (params) Object.entries(params).forEach(([k, v]) => u.searchParams.set(k, v));
  const r = await fetch(u.toString(), {
    headers: {
      'User-Agent': UA,
      'Accept': 'application/json',
      'Cookie': 'arl=' + ARL
    }
  });
  if (!r.ok) throw new Error('Deezer HTTP ' + r.status);
  return r.json();
}

async function deezerFindBestTrack(title, artist, isrc, env) {
  if (!title) return null;
  const norm = s => String(s || '').toLowerCase().replace(/\s+/g, ' ').trim();
  const wantTitle  = norm(title);
  const wantArtist = norm(artist || '');

  // ISRC fast path
  if (isrc) {
    try {
      const data = await deezerApi(env, '/search', { q: 'isrc:"' + isrc + '"' });
      const items = (data && data.data) || [];
      const match = items.find(t => t.isrc && t.isrc.toUpperCase() === isrc.toUpperCase());
      if (match) return match;
    } catch {}
  }

  // Title + artist search
  const q = (artist ? artist + ' ' : '') + title;
  const data = await deezerApi(env, '/search', { q });
  const items = (data && data.data) || [];
  if (!items.length) return null;

  const scored = items
    .map(t => {
      const tTitle  = norm(t.title || '');
      const tArtist = norm((t.artist && t.artist.name) || '');
      let score = 0;
      if (tTitle === wantTitle) score += 5;
      if (wantArtist && tArtist === wantArtist) score += 5;
      if (wantTitle && tTitle.includes(wantTitle)) score += 2;
      if (wantArtist && tArtist.includes(wantArtist)) score += 2;
      return { t, score };
    })
    .sort((a, b) => b.score - a.score);

  return scored[0] && scored[0].score > 0 ? scored[0].t : null;
}

async function deezerStream(deezerTrackId, env) {
  const data = await deezerApi(env, '/track/' + deezerTrackId, {});
  // NOTE: Deezer's public API returns a 30s preview in data.preview.
  // If you use a private endpoint for full streams, wire that here.
  const url = (data && (data.stream_url || data.url || data.preview)) || null;
  if (!url) throw new Error('No Deezer stream URL');
  const isFlac = data && data.type && String(data.type).toLowerCase().includes('flac');
  return {
    url,
    format: isFlac ? 'flac' : 'mp3',
    quality: isFlac ? 'lossless' : 'high',
    source: 'deezer',
    expiresAt: Math.floor(Date.now() / 1000) + 7200
  };
}

// ─── Middleware helpers ──────────────────────────────────────────────────────
async function withToken(c, fn) {
  const token = c.req.param('token');
  const entry = await getTokenEntry(c.env, token);
  if (!entry) return c.json({ error: 'Invalid token.' }, 404);
  if (!checkRateLimit(entry)) return c.json({ error: 'Rate limit exceeded.' }, 429);
  if (entry.reqCount % 20 === 0) redisSave(c.env, token, entry).catch(() => {});
  return fn(entry);
}

// ─── Config + health + manifest ──────────────────────────────────────────────
app.get('/', c => {
  const base = getBaseUrl(c);
  return c.html(
    `<html><body style="font-family:sans-serif;background:#111;color:#eee;padding:20px">
      <h1>SoundCloud + Deezer (SNIP-only) Addon</h1>
      <p>Use <code>POST ${base}/generate</code> to get your Eclipse addon URL.</p>
      <p>Manifest: <code>${base}/u/&lt;token&gt;/manifest.json</code></p>
    </body></html>`
  );
});

app.get('/health', c => c.json({
  status: 'ok',
  sharedClientIdReady: !!SHARED_CLIENT_ID,
  redisConfigured: !!(c.env.REDIS_URL && c.env.REDIS_TOKEN),
  timestamp: new Date().toISOString()
}));

app.post('/generate', async c => {
  const ip     = (c.req.header('cf-connecting-ip') || c.req.header('x-forwarded-for') || 'unknown').split(',')[0].trim();
  const bucket = getOrCreateIpBucket(ip);
  if (bucket.count >= MAX_TOKENS_PER_IP) return c.json({ error: 'Too many tokens today from this IP.' }, 429);

  const b   = await c.req.json().catch(() => ({}));
  const cid = b.clientId ? String(b.clientId).trim() : null;
  if (cid && !/^[a-zA-Z0-9]{20,40}$/.test(cid)) return c.json({ error: 'Invalid client_id.' }, 400);

  const token = generateToken();
  const oauth = b.oauthToken ? String(b.oauthToken).trim() : null;
  const entry = {
    clientId:   cid || null,
    oauthToken: oauth || null,
    createdAt:  Date.now(),
    lastUsed:   Date.now(),
    reqCount:   0,
    rateWin:    []
  };
  TOKEN_CACHE.set(token, entry);
  await redisSave(c.env, token, entry);
  bucket.count++;

  if (!SHARED_CLIENT_ID && !c.env.SC_CLIENT_ID) {
    c.executionCtx.waitUntil(fetchSharedClientId(c.env));
  }

  return c.json({ token, manifestUrl: getBaseUrl(c) + '/u/' + token + '/manifest.json' });
});

app.get('/u/:token/manifest.json', async c =>
  withToken(c, () => c.json({
    id:          'com.eclipse.soundcloud.deezer.' + c.req.param('token').slice(0, 8),
    name:        'SoundCloud',
    version:     '1.0.0',
    description: 'SoundCloud streams with Deezer fallback for 30-second previews.',
    icon:        'https://files.softicons.com/download/social-media-icons/simple-icons-by-dan-leech/png/128x128/soundcloud.png',
    resources:   ['search', 'stream', 'catalog'],
    types:       ['track', 'album', 'artist', 'playlist']
  }))
);

// ─── Search ───────────────────────────────────────────────────────────────────
app.get('/u/:token/search', async c =>
  withToken(c, async entry => {
    const q = cleanText(c.req.query('q'));
    if (!q) return c.json({ tracks: [], albums: [], artists: [], playlists: [] });

    if (!SHARED_CLIENT_ID && !c.env.SC_CLIENT_ID) {
      await fetchSharedClientId(c.env);
    }
    const cid = effectiveCid(entry, c.env);
    if (!cid) return c.json({ error: 'No client_id yet. Retry.' }, 503);

    try {
      const [trackRes, plRes, userRes] = await Promise.all([
        scGet(cid, 'https://api-v2.soundcloud.com/search/tracks',    { q, limit: 20, offset: 0, linked_partitioning: 1 }, scOAuth(entry)).catch(() => null),
        scGet(cid, 'https://api-v2.soundcloud.com/search/playlists', { q, limit: 10, offset: 0 },                       scOAuth(entry)).catch(() => null),
        scGet(cid, 'https://api-v2.soundcloud.com/search/users',     { q, limit: 5,  offset: 0 },                       scOAuth(entry)).catch(() => null)
      ]);

      const allPl = (plRes && plRes.collection) || [];
      const tracks = ((trackRes && trackRes.collection) || []).map(t => {
        rememberTrack(t);
        const m = parseArtistTitle(t);
        return {
          id:         String(t.id),
          title:      m.title || 'Unknown',
          artist:     m.artist || 'Unknown',
          album:      null,
          duration:   t.duration ? Math.floor(t.duration / 1000) : null,
          artworkURL: artworkUrl(t.artwork_url),
          format:     'aac'
        };
      });

      const albums = allPl
        .filter(p => p.is_album && p.track_count > 0)
        .map(p => ({
          id:         String(p.id),
          title:      p.title || 'Unknown',
          artist:     (p.user && p.user.username) || 'Unknown',
          artworkURL: artworkUrl(p.artwork_url),
          trackCount: p.track_count || null,
          year:       scYear(p)
        }));

      const playlists = allPl
        .filter(p => !p.is_album && p.track_count > 0)
        .map(p => ({
          id:         String(p.id),
          title:      p.title || 'Unknown',
          description: p.description || null,
          artworkURL: artworkUrl(p.artwork_url),
          creator:    (p.user && p.user.username) || null,
          trackCount: p.track_count || null,
          year:       scYear(p)
        }));

      const artists = ((userRes && userRes.collection) || []).map(u => ({
        id:         String(u.id),
        name:       u.username || 'Unknown',
        artworkURL: artworkUrl(u.avatar_url),
        genres:     u.genre ? [u.genre] : []
      }));

      return c.json({ tracks, albums, artists, playlists });
    } catch (e) {
      return c.json({ error: 'Search failed.', tracks: [] }, 500);
    }
  })
);

// ─── Stream ───────────────────────────────────────────────────────────────────
app.get('/u/:token/stream/:id', async c =>
  withToken(c, async entry => {
    if (!SHARED_CLIENT_ID && !c.env.SC_CLIENT_ID) {
      await fetchSharedClientId(c.env);
    }
    const cid = effectiveCid(entry, c.env);
    const tid = c.req.param('id');
    if (!cid) return c.json({ error: 'No client_id available.' }, 503);

    return dedupeCall('stream:' + tid, async () => {
      let track = null;
      const cached = TRACK_CACHE.get(String(tid)) || null;
      try {
        try { track = await scGet(cid, 'https://api-v2.soundcloud.com/tracks/soundcloud:tracks:' + tid, {}, scOAuth(entry)); }
        catch { track = await scGet(cid, 'https://api-v2.soundcloud.com/tracks/' + tid, {}, scOAuth(entry)); }
      } catch {}
      if (track) rememberTrack(track);

      const meta = cached || (track ? parseArtistTitle(track) : null);
      const liveTc = (track && track.media && track.media.transcodings) || [];
      const isSnippet = !!(track && track.policy === 'SNIP');

      async function resolveTranscoding(transcoding, preview) {
        const sd = await scGet(cid, transcoding.url, {}, scOAuth(entry));
        if (!sd || !sd.url) return null;
        const mime   = (transcoding.format && transcoding.format.mime_type) || '';
        const isOpus = mime.includes('opus');
        const isAac  = mime.includes('aac') || mime.includes('mp4');
        const fmt    = isOpus ? 'opus' : isAac ? 'aac' : 'mp3';
        return {
          url:        sd.url,
          format:     fmt,
          quality:    preview ? 'preview' : (isOpus ? '64kbps' : '128kbps'),
          source:     'soundcloud',
          isPreview:  preview || false,
          expiresAt:  Math.floor(Date.now() / 1000) + 86400
        };
      }

      if (isSnippet) {
        console.log('[Stream] SNIP policy for', tid, '— trying Deezer fallback');

        if (meta && (meta.title || meta.rawTitle)) {
          try {
            const dzTrack = await deezerFindBestTrack(
              meta.title || meta.rawTitle,
              meta.artist,
              (meta.isrc || null),
              c.env
            );
            if (dzTrack && dzTrack.id) {
              const dzStream = await deezerStream(dzTrack.id, c.env);
              if (dzStream && dzStream.url) {
                console.log('[Stream] SNIP fallback → Deezer OK', tid);
                return c.json(dzStream);
              }
            }
          } catch (e) {
            console.log('[Stream] SNIP Deezer failed:', e.message);
          }
        }

        console.log('[Stream] SNIP Deezer failed for', tid, '— serving SC snippet as last resort');
        for (const t of liveTc) {
          try {
            const r = await resolveTranscoding(t, true);
            if (r) return c.json(r);
          } catch {}
        }
        return c.json({ error: 'No full stream found for snippet track ' + tid }, 404);
      }

      // Non-SNIP: SC ONLY. No Deezer fallback.
      if (liveTc.length > 0) {
        const progressives = liveTc.filter(t => t.format && t.format.protocol === 'progressive');
        for (const t of progressives) {
          try {
            const r = await resolveTranscoding(t, false);
            if (r) return c.json(r);
          } catch {}
        }
        const hlsTracks = liveTc.filter(t => t.format && t.format.protocol === 'hls');
        for (const t of hlsTracks) {
          try {
            const r = await resolveTranscoding(t, false);
            if (r) return c.json({ ...r, isHls: true });
          } catch {}
        }
        for (const t of liveTc) {
          try {
            const r = await resolveTranscoding(t, false);
            if (r) return c.json(r);
          } catch {}
        }
        return c.json({ error: 'SoundCloud stream unavailable for track ' + tid }, 404);
      }

      return c.json({ error: 'No stream found for track ' + tid }, 404);
    });
  })
);

export default app;
