const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http);
const { createClient } = require('@supabase/supabase-js');

app.use(express.static(__dirname));

const SUPABASE_BUCKET = process.env.SUPABASE_BUCKET || 'Elsewhere_Together';
const supabase = (process.env.SUPABASE_URL && process.env.SUPABASE_KEY)
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY)
  : null;

// Gallery routes
app.get('/gallery', (req, res) => res.sendFile(__dirname + '/gallery.html'));

app.get('/gallery-data', async (req, res) => {
  if (!supabase) return res.json([]);
  try {
    const { data, error } = await supabase.storage
      .from(SUPABASE_BUCKET)
      .list('', { limit: 200, sortBy: { column: 'created_at', order: 'desc' } });
    if (error || !data) return res.json([]);
    const items = data
      .filter(f => f.name.endsWith('.png'))
      .map(f => {
        const { data: u } = supabase.storage.from(SUPABASE_BUCKET).getPublicUrl(f.name);
        return { url: u.publicUrl, name: f.name };
      });
    res.json(items);
  } catch { res.json([]); }
});

// ── WebRTC ICE servers ──────────────────────────────────────────
// Cloudflare Realtime TURN gives each client short-lived, single-use
// credentials (unlike the old static public demo credentials, which are
// shared with every other project using them). Falls back to the old
// public STUN/TURN set if Cloudflare isn't configured or the request fails,
// so the app still works (just with less TURN headroom) either way.
const CF_TURN_KEY_ID = process.env.CF_TURN_KEY_ID;
const CF_TURN_TOKEN  = process.env.CF_TURN_TOKEN;
const FALLBACK_ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
  { urls: 'turn:openrelay.metered.ca:80',  username: 'openrelayproject', credential: 'openrelayproject' },
  { urls: 'turn:openrelay.metered.ca:443', username: 'openrelayproject', credential: 'openrelayproject' },
  { urls: 'turn:openrelay.metered.ca:443?transport=tcp', username: 'openrelayproject', credential: 'openrelayproject' }
];

app.get('/ice-servers', async (req, res) => {
  if (!CF_TURN_KEY_ID || !CF_TURN_TOKEN) {
    return res.json({ iceServers: FALLBACK_ICE_SERVERS });
  }
  try {
    const cfRes = await fetch(
      `https://rtc.live.cloudflare.com/v1/turn/keys/${CF_TURN_KEY_ID}/credentials/generate-ice-servers`,
      {
        method: 'POST',
        headers: { Authorization: `Bearer ${CF_TURN_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ ttl: 86400 })
      }
    );
    if (!cfRes.ok) throw new Error(`Cloudflare TURN request failed: ${cfRes.status}`);
    const data = await cfRes.json();
    console.log('Cloudflare TURN credentials issued OK');
    res.json({ iceServers: data.iceServers });
  } catch (err) {
    console.error('Cloudflare TURN credential fetch failed, using fallback:', err.message);
    res.json({ iceServers: FALLBACK_ICE_SERVERS });
  }
});

const PROMPTS = {
  en: [
    "Draw your favorite snacks you used to buy with your pocket money.",
    "Draw a table set with your favorite childhood meals.",
    "Draw a game you used to play with your friends.",
    "Draw a memorable childhood birthday.",
    "Draw a snowman you made in your childhood.",
    "Draw your favorite childhood toys and dolls.",
    "Draw the day you learned to ride a bike.",
    "Draw your favorite thing you had in your childhood bedroom.",
    "Draw the playground you used to play in as a child.",
    "Draw the favorite outfit or pair of shoes you wore as a kid.",
    "Draw your favorite cartoon character from your childhood.",
    "Draw an object that always sat on your grandparents' table or shelf.",
    "Draw your favorite corner of your house.",
    "Draw the backpack or lunchbox you used to take to school.",
    "Draw the family car you grew up taking trips in.",
    "Draw a specific piece of furniture from your childhood living room.",
    "Draw the blanket, pillow, or stuffed animal that helped you sleep as a child.",
    "Draw a sandcastle, snow fort, or Lego structure you built in your childhood.",
    "Draw your first day of school.",
    "Draw the plants you have or would like to have."
  ],
  de: [
    "Zeichne deine Lieblingssnacks, die du von deinem Taschengeld gekauft hast.",
    "Zeichne einen gedeckten Tisch mit deinen Lieblingsessen aus der Kindheit.",
    "Zeichne ein Spiel, das du als Kind mit deinen Freunden gespielt hast.",
    "Zeichne einen unvergesslichen Kindergeburtstag.",
    "Zeichne einen Schneemann, den du als Kind gebaut hast.",
    "Zeichne deine Lieblingsspielzeuge und -puppen aus der Kindheit.",
    "Zeichne den Tag, an dem du Fahrradfahren gelernt hast.",
    "Zeichne dein Lieblingsstück aus deinem Kinderzimmer.",
    "Zeichne den Spielplatz, auf dem du als Kind gespielt hast.",
    "Zeichne dein Lieblingsoutfit oder deine Lieblingsschuhe aus der Kindheit.",
    "Zeichne deine Lieblings-Zeichentrickfigur aus deiner Kindheit.",
    "Zeichne einen Gegenstand, der immer auf dem Tisch oder Regal deiner Großeltern stand.",
    "Zeichne deine Lieblingsecke in deinem Zuhause.",
    "Zeichne den Schulranzen oder die Brotdose, die du mit in die Schule genommen hast.",
    "Zeichne das Familienauto, mit dem ihr früher Ausflüge gemacht habt.",
    "Zeichne ein bestimmtes Möbelstück aus dem Wohnzimmer deiner Kindheit.",
    "Zeichne die Decke, das Kissen oder das Kuscheltier, das dir als Kind beim Einschlafen geholfen hat.",
    "Zeichne eine Sandburg, eine Schneeburg oder ein Lego-Bauwerk, das du als Kind gebaut hast.",
    "Zeichne deinen ersten Schultag.",
    "Zeichne die Pflanzen, die du hast oder gerne hättest."
  ]
};

let session = {
  clients: [],
  state: 'idle',
  initiatorId: null,
  waitingTimer: null,
  phaseTimer: null,
  promptIndex: null,
  strokes: [],
  cameraReady: new Set(),
  talkResponses: {},
  talkingEndsAt: null
};

function broadcast(event, data) {
  session.clients.forEach(c => c.emit(event, data));
}

// Tells each client the OTHER client's city name (each kiosk already knows
// its own from its own URL's ?city=). Re-run whenever the roster changes.
function broadcastCities() {
  session.clients.forEach(c => {
    const partner = session.clients.find(o => o !== c);
    c.emit('partner-city', { city: partner ? partner._city : null });
  });
}

function clearTimers() {
  clearTimeout(session.waitingTimer);
  clearTimeout(session.phaseTimer);
  session.waitingTimer = null;
  session.phaseTimer = null;
}

function resetToIdle() {
  clearTimers();
  session.state = 'idle';
  session.initiatorId = null;
  session.promptIndex = null;
  session.strokes = [];
  session.talkResponses = {};
  session.talkingEndsAt = null;
  broadcast('phase', { phase: 'idle' });
  // Re-establish WebRTC after session reset — cameras are still running
  if (session.clients.length === 2) {
    session.clients[0].emit('peer-ready', { initiator: true });
    session.clients[1].emit('peer-ready', { initiator: false });
  }
}

function tryPeerReady() {
  if (session.clients.length === 2 &&
      session.clients.every(c => session.cameraReady.has(c.id))) {
    session.clients[0].emit('peer-ready', { initiator: true });
    session.clients[1].emit('peer-ready', { initiator: false });
  }
}

function pickPromptIndex() {
  return Math.floor(Math.random() * PROMPTS.en.length);
}

function enterTalkAsk() {
  session.state = 'talk-ask';
  session.talkResponses = {};
  broadcast('phase', { phase: 'talk-ask' });
  session.phaseTimer = setTimeout(() => {
    // Not both answered "yes" in time — back to idle
    resetToIdle();
  }, 15000);
}

function enterTalking() {
  // Cancel the talk-ask fallback timer — it's a separate setTimeout from a
  // prior phase, and reassigning session.phaseTimer below doesn't stop it.
  clearTimeout(session.phaseTimer);
  session.state = 'talking';
  broadcast('phase', { phase: 'talking' });
  session.talkingEndsAt = Date.now() + 45000;
  session.phaseTimer = setTimeout(() => {
    resetToIdle();
  }, 45000);
}

function runPhaseSequence() {
  session.strokes = [];
  session.promptIndex = pickPromptIndex();

  const p = { en: PROMPTS.en[session.promptIndex], de: PROMPTS.de[session.promptIndex] };

  // Onboarding: 45s
  session.state = 'onboarding';
  broadcast('phase', { phase: 'onboarding' });

  session.phaseTimer = setTimeout(() => {
    // Connection: 8s
    session.state = 'connection';
    broadcast('phase', { phase: 'connection' });

    session.phaseTimer = setTimeout(() => {
      // Prompt display: 5s
      session.state = 'prompt';
      broadcast('phase', { phase: 'prompt', prompt: p });

      session.phaseTimer = setTimeout(() => {
        // Drawing: 4 minutes
        session.state = 'drawing';
        broadcast('phase', { phase: 'drawing', prompt: p });

        session.phaseTimer = setTimeout(() => {
          // Closure I – photo consent: 10s
          session.state = 'closure1';
          broadcast('phase', { phase: 'closure1' });

          session.phaseTimer = setTimeout(() => {
            // Closure II – final: 10s
            session.state = 'closure2';
            broadcast('phase', { phase: 'closure2', prompt: p, initiatorId: session.initiatorId });

            session.phaseTimer = setTimeout(() => {
              enterTalkAsk();
            }, 10000);
          }, 10000);
        }, 240000);
      }, 5000);
    }, 8000);
  }, 45000);
}

io.on('connection', (socket) => {
  const clientId = socket.handshake.query.clientId || null;
  const loadId   = socket.handshake.query.loadId   || null;
  socket._clientId = clientId;
  socket._loadId   = loadId;
  socket._city     = socket.handshake.query.city || null;

  // Check if this clientId is already in the session
  const existingIdx = clientId
    ? session.clients.findIndex(c => c._clientId === clientId)
    : -1;

  if (existingIdx !== -1) {
    const existing = session.clients[existingIdx];
    if (existing._loadId === loadId) {
      // Same page load — genuine network reconnect, restore their slot
      console.log('Network reconnect for clientId:', clientId);
      session.cameraReady.delete(existing.id);
      session.clients[existingIdx] = socket;
    } else {
      // Different loadId — intentional page refresh, treat as leaving
      console.log('Page refresh for clientId:', clientId, '— resetting session');
      session.cameraReady.delete(existing.id);
      session.clients = session.clients.filter((_, i) => i !== existingIdx);
      // The refreshed tab's old RTCPeerConnection is gone, but the survivor's
      // is still alive from its point of view (state stays 'connected'/'disconnected',
      // never 'failed') — tell it to close now so it rebuilds once peer-ready fires.
      session.clients.forEach(c => c.emit('webrtc-reset'));
      if (session.state !== 'idle') resetToIdle();
      session.clients.push(socket);
    }
  } else if (session.clients.length >= 2) {
    socket.emit('full');
    socket.disconnect();
    return;
  } else {
    session.clients.push(socket);
  }

  console.log('Tab connected:', socket.id, '| Clients:', session.clients.length);
  broadcastCities();

  // Sync new tab to current state
  const p = session.promptIndex !== null
    ? { en: PROMPTS.en[session.promptIndex], de: PROMPTS.de[session.promptIndex] }
    : null;
  socket.emit('phase', { phase: session.state, prompt: p });

  // If someone is already waiting, tell the newcomer
  if (session.state === 'waiting') {
    socket.emit('partner-waiting');
  }

  // WebRTC starts only once both cameras confirm ready (via 'camera-ready' event)
  // If a second client just joined and the first already has a camera, check now
  if (session.clients.length === 2) tryPeerReady();

  // If we're in the drawing phase, send existing strokes so the canvas is in sync
  if (session.state === 'drawing' && session.strokes.length > 0) {
    socket.emit('strokes-sync', session.strokes);
  }

  // ── START button ──────────────────────────────────────────────
  socket.on('start', () => {
    if (session.state === 'idle') {
      session.state = 'waiting';
      session.initiatorId = socket.id;
      socket.emit('phase', { phase: 'waiting', role: 'initiator' });
      socket.broadcast.emit('partner-waiting');
      session.waitingTimer = setTimeout(() => {
        if (session.state === 'waiting') resetToIdle();
      }, 30000);

    } else if (session.state === 'waiting' && socket.id !== session.initiatorId) {
      clearTimeout(session.waitingTimer);
      session.waitingTimer = null;
      runPhaseSequence();
    }
  });

  // ── Drawing ───────────────────────────────────────────────────
  socket.on('draw', (seg) => {
    seg.owner = socket.id;
    session.strokes.push(seg);
    socket.broadcast.emit('draw', seg);
  });

  socket.on('clear-my-drawing', () => {
    session.strokes = session.strokes.filter(s => s.owner !== socket.id);
    broadcast('clear-owner', { owner: socket.id });
  });

  // Only removes a stroke owned by the requesting socket — never a partner's.
  // Only broadcasts if something was actually removed, so a stale strokeId
  // can't desync clients from the server's authoritative strokes list.
  socket.on('undo-stroke', ({ strokeId }) => {
    const before = session.strokes.length;
    session.strokes = session.strokes.filter(s => !(s.strokeId === strokeId && s.owner === socket.id));
    if (session.strokes.length !== before) {
      broadcast('undo-stroke', { strokeId });
    }
  });

  // ── Gesture (emoji reaction) ──────────────────────────────────
  socket.on('gesture', (data) => {
    broadcast('gesture', data);
  });

  // ── Photo consent (closure1) — relay each side's yes/no to the other ──
  socket.on('photo-consent', ({ consent }) => {
    const partner = session.clients.find(c => c.id !== socket.id);
    if (partner) partner.emit('partner-photo-consent', { consent });
  });

  // ── Talking controls — either side can hang up or add 15s ─────
  socket.on('talking-hangup', () => {
    if (session.state === 'talking') resetToIdle();
  });

  socket.on('talking-extend', () => {
    if (session.state !== 'talking') return;
    clearTimeout(session.phaseTimer);
    session.talkingEndsAt += 15000;
    session.phaseTimer = setTimeout(() => {
      resetToIdle();
    }, Math.max(0, session.talkingEndsAt - Date.now()));
    broadcast('talking-extended', {});
  });

  // ── Save artwork ─────────────────────────────────────────────
  socket.on('save-artwork', async ({ image }) => {
    if (!supabase) return;
    try {
      const buf = Buffer.from(image.replace(/^data:image\/png;base64,/, ''), 'base64');
      const filename = `${Date.now()}.png`;
      const { error } = await supabase.storage
        .from(SUPABASE_BUCKET)
        .upload(filename, buf, { contentType: 'image/png' });
      if (error) console.error('Upload error:', error.message);
      else console.log('Artwork saved:', filename);
    } catch (err) { console.error('Save artwork failed:', err.message); }
  });

  // ── Post-drawing "talk to your partner?" consent ───────────────
  socket.on('talk-response', ({ wantsTalk }) => {
    if (session.state !== 'talk-ask') return;
    session.talkResponses[socket.id] = wantsTalk;

    if (!wantsTalk) {
      resetToIdle();
      return;
    }
    if (session.clients.length === 2 &&
        session.clients.every(c => session.talkResponses[c.id] === true)) {
      enterTalking();
    }
  });

  // ── Camera ready (WebRTC gating) ──────────────────────────────
  socket.on('camera-ready', () => {
    session.cameraReady.add(socket.id);
    console.log('Camera ready:', socket.id, '| Ready count:', session.cameraReady.size);
    tryPeerReady();
  });

  // ── WebRTC signalling relay ───────────────────────────────────
  socket.on('webrtc-offer',   (d) => socket.broadcast.emit('webrtc-offer',   d));
  socket.on('webrtc-answer',  (d) => socket.broadcast.emit('webrtc-answer',  d));
  socket.on('webrtc-ice',     (d) => socket.broadcast.emit('webrtc-ice',     d));


  // ── Disconnect ────────────────────────────────────────────────
  socket.on('disconnect', () => {
    console.log('Tab disconnected:', socket.id, '| clientId:', socket._clientId);
    session.cameraReady.delete(socket.id);

    // Don't remove if this slot was already replaced by a reconnect
    session.clients = session.clients.filter(c => c.id !== socket.id);

    // Whatever remains has no other way to learn the far end just vanished —
    // its RTCPeerConnection would otherwise sit stale (still 'connected' or
    // 'disconnected', never 'failed') until the partner reconnects. This fires
    // for real leaves, refreshes, and network blips alike; a reconnecting
    // partner re-announces 'camera-ready' and peer-ready rebuilds it cleanly.
    session.clients.forEach(c => c.emit('webrtc-reset'));
    broadcastCities();

    if (session.state !== 'idle') {
      // Short grace period for network blips — if the same loadId reconnects, the
      // connection handler will restore their slot and cancel the reset naturally
      const snapClientId = socket._clientId;
      const snapLoadId   = socket._loadId;
      setTimeout(() => {
        const reconnected = session.clients.some(
          c => c._clientId === snapClientId && c._loadId === snapLoadId
        );
        if (!reconnected && session.state !== 'idle') {
          console.log('Client did not reconnect, resetting to idle');
          resetToIdle();
        }
      }, 3000);
    }
  });
});

const PORT = process.env.PORT || 3000;
http.listen(PORT, () => {
  console.log(`\n✅  Server running on port ${PORT}`);
  if (PORT === 3000) console.log('    Open TWO browser tabs at http://localhost:3000\n');
});
