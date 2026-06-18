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

const PROMPTS = {
  en: [
    "Draw your favorite snacks you used to buy with your pocket money.",
    "Draw a table set with your favorite childhood meals.",
    "Draw what home means to you.",
    "Draw a memorable childhood birthday.",
    "Draw a snowman you made in your childhood.",
    "Draw your favorite childhood toys and dolls.",
    "Draw the day you learned to ride a bike.",
    "Draw some memorable gifts you received.",
    "Draw the playground you used to play in as a child.",
    "Draw the favorite outfit or pair of shoes you wore as a kid.",
    "Draw a gesture that means welcome.",
    "Draw an object that always sat on your grandparents' table or shelf.",
    "Draw a memorable family gathering you remember.",
    "Draw the backpack or lunchbox you used to take to school.",
    "Draw the family car you grew up taking trips in.",
    "Draw a specific piece of furniture from your childhood living room.",
    "Draw the blanket, pillow, or stuffed animal that helped you sleep as a child.",
    "Draw someone you miss.",
    "Draw your first day of school.",
    "Draw the plants you have or would like to have."
  ],
  de: [
    "Zeichne deine Lieblingssnacks, die du dir mit Taschengeld gekauft hast.",
    "Zeichne einen Tisch mit deinen Lieblingsgerichten aus der Kindheit.",
    "Zeichne, was Zuhause für dich bedeutet.",
    "Zeichne einen unvergesslichen Kindergeburtstag.",
    "Zeichne einen Schneemann aus deiner Kindheit.",
    "Zeichne dein liebstes Spielzeug aus der Kindheit.",
    "Zeichne den Tag, an dem du Fahrrad fahren gelernt hast.",
    "Zeichne unvergessliche Geschenke, die du bekommen hast.",
    "Zeichne den Spielplatz, auf dem du als Kind gespielt hast.",
    "Zeichne dein Lieblingsoutfit oder deine Lieblingsschuhe aus der Kindheit.",
    "Zeichne eine Geste, die Willkommen bedeutet.",
    "Zeichne einen Gegenstand vom Tisch oder Regal deiner Großeltern.",
    "Zeichne ein unvergessliches Familientreffen.",
    "Zeichne den Schulrucksack oder die Brotdose aus deiner Schulzeit.",
    "Zeichne das Familienauto, mit dem ihr Ausflüge gemacht habt.",
    "Zeichne ein Möbelstück aus dem Wohnzimmer deiner Kindheit.",
    "Zeichne die Decke oder das Kuscheltier, das dir als Kind beim Schlafen half.",
    "Zeichne jemanden, den du vermisst.",
    "Zeichne deinen ersten Schultag.",
    "Zeichne die Pflanzen, die du hast oder haben möchtest."
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
  cameraReady: new Set()
};

function broadcast(event, data) {
  session.clients.forEach(c => c.emit(event, data));
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

function runPhaseSequence() {
  session.strokes = [];
  session.promptIndex = pickPromptIndex();

  const p = { en: PROMPTS.en[session.promptIndex], de: PROMPTS.de[session.promptIndex] };

  // Onboarding: 54s
  session.state = 'onboarding';
  broadcast('phase', { phase: 'onboarding' });

  session.phaseTimer = setTimeout(() => {
    // Connection: 15s
    session.state = 'connection';
    broadcast('phase', { phase: 'connection' });

    session.phaseTimer = setTimeout(() => {
      // Prompt display: 10s
      session.state = 'prompt';
      broadcast('phase', { phase: 'prompt', prompt: p });

      session.phaseTimer = setTimeout(() => {
        // Drawing: 4 minutes
        session.state = 'drawing';
        broadcast('phase', { phase: 'drawing', prompt: p });

        session.phaseTimer = setTimeout(() => {
          // Closure I – photo warning: 10s
          session.state = 'closure1';
          broadcast('phase', { phase: 'closure1' });

          session.phaseTimer = setTimeout(() => {
            // Closure II – final: 10s
            session.state = 'closure2';
            broadcast('phase', { phase: 'closure2', prompt: p, initiatorId: session.initiatorId });

            session.phaseTimer = setTimeout(() => {
              resetToIdle();
            }, 10000);
          }, 10000);
        }, 240000);
      }, 10000);
    }, 15000);
  }, 44000);
}

io.on('connection', (socket) => {
  console.log('Tab connected:', socket.id, '| Clients:', session.clients.length + 1);

  if (session.clients.length >= 2) {
    socket.emit('full');
    socket.disconnect();
    return;
  }

  session.clients.push(socket);

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

  // ── Gesture (emoji reaction) ──────────────────────────────────
  socket.on('gesture', (data) => {
    broadcast('gesture', data);
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
    console.log('Tab disconnected:', socket.id);
    session.cameraReady.delete(socket.id);
    session.clients = session.clients.filter(c => c.id !== socket.id);
    if (session.state !== 'idle') resetToIdle();
  });
});

const PORT = process.env.PORT || 3000;
http.listen(PORT, () => {
  console.log(`\n✅  Server running on port ${PORT}`);
  if (PORT === 3000) console.log('    Open TWO browser tabs at http://localhost:3000\n');
});
