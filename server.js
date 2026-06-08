const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);

const CORS_ORIGINS = process.env.CORS_ORIGINS
  ? process.env.CORS_ORIGINS.split(',').map(s => s.trim())
  : ['http://localhost:5173', 'http://localhost:3000', 'http://localhost:5174'];

const io = new Server(server, {
  maxHttpBufferSize: 5e6,
  cors: {
    origin: CORS_ORIGINS,
    methods: ['GET', 'POST'],
  },
});

app.use(cors({ origin: CORS_ORIGINS }));
app.use(express.json({ limit: '5mb' }));

const limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 200,
  message: { error: 'Too many requests.' },
});
app.use(limiter);

// ── In-memory store ──────────────────────────────────────────────────────────
const rooms   = {};   // roomId -> { creatorToken, messages[], members: Set<socketId>, memberMeta: Map<socketId, userObj>, cleanupTimeout }
const admins  = new Set();  // admin socket IDs
const MAX_MSG_LENGTH   = 300;
const MAX_ROOM_MESSAGES = 200;
const ROOM_LIFETIME_MS  = 15 * 60 * 1000;

function sanitize(str) {
  return String(str)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#x27;').trim();
}

// Broadcast current state to all admin sockets
function broadcastAdminState() {
  const state = { rooms: {} };
  for (const [roomId, room] of Object.entries(rooms)) {
    state.rooms[roomId] = {
      roomId,
      createdAt: room.createdAt,
      members: Array.from(room.memberMeta.values()),
      messages: room.messages.slice(-50),
    };
  }
  for (const adminId of admins) {
    io.to(adminId).emit('admin-state', state);
  }
}

function emitToAdmins(event, data) {
  for (const adminId of admins) {
    io.to(adminId).emit(event, data);
  }
}

// ── REST API ─────────────────────────────────────────────────────────────────
app.post('/api/rooms/create', (req, res) => {
  const roomId = `love-${crypto.randomBytes(3).toString('hex')}`;
  const creatorToken = crypto.randomBytes(16).toString('hex');

  rooms[roomId] = {
    creatorToken,
    messages: [],
    members: new Set(),
    memberMeta: new Map(),
    blockedUsers: new Set(),
    cleanupTimeout: null,
    createdAt: Date.now(),
  };

  console.log(`[Room Created] ${roomId}`);
  emitToAdmins('admin-room-created', { roomId, createdAt: rooms[roomId].createdAt });
  res.json({ roomId, creatorToken });
});

app.get('/api/rooms/:roomId', (req, res) => {
  res.json({ exists: !!rooms[req.params.roomId] });
});

app.delete('/api/rooms/:roomId', (req, res) => {
  const { roomId } = req.params;
  if (rooms[roomId]) {
    clearTimeout(rooms[roomId].cleanupTimeout);
    io.to(roomId).emit('room-ended');
    delete rooms[roomId];
    emitToAdmins('admin-room-deleted', { roomId });
    console.log(`[Room Deleted] ${roomId} (API)`);
  }
  res.json({ success: true });
});

app.get('/health', (_, res) =>
  res.json({ status: 'ok', rooms: Object.keys(rooms).length, admins: admins.size })
);

// ── Serve frontend SPAs ──────────────────────────────────────────────────────
const frontendDist = path.join(__dirname, '..', 'frontend', 'dist');
const adminDist = path.join(__dirname, '..', 'frontend-admin', 'dist');

if (fs.existsSync(adminDist)) {
  app.use('/admin', express.static(adminDist, { maxAge: '1y' }));
  app.get('/admin/*', (req, res) => {
    res.sendFile(path.join(adminDist, 'index.html'));
  });
}

if (fs.existsSync(frontendDist)) {
  app.use(express.static(frontendDist, { maxAge: '1y' }));
  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/api') || req.path.startsWith('/socket.io')) return next();
    res.sendFile(path.join(frontendDist, 'index.html'));
  });
}

// ── Socket.IO ────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[Connect] ${socket.id}`);

  let currentRoom  = null;
  let currentName  = 'Anonymous';
  let currentClientId = '';
  let isCreator    = false;
  let isAdmin      = false;

  const msgTimestamps = [];
  const MSG_RATE_LIMIT = 5;
  const MSG_RATE_WINDOW = 2000;

  // ── ADMIN ──────────────────────────────────────────────────────────────────
  socket.on('admin-join', () => {
    isAdmin = true;
    admins.add(socket.id);
    console.log(`[Admin Connected] ${socket.id}`);
    broadcastAdminState();
  });

  // Admin: kick a user
  socket.on('admin-kick-user', ({ socketId, roomId }) => {
    if (!isAdmin) return;
    const target = io.sockets.sockets.get(socketId);
    if (target) {
      target.emit('kicked', { reason: 'You were removed by the admin.' });
      target.disconnect(true);
    }
  });

  // Admin: block a user (prevents rejoin by clientId)
  socket.on('admin-block-user', ({ socketId, roomId }) => {
    if (!isAdmin) return;
    if (!rooms[roomId]) return;
    const room = rooms[roomId];
    const meta = room.memberMeta.get(socketId);
    const blockId = meta?.clientId || socketId;
    room.blockedUsers.add(blockId);
    const target = io.sockets.sockets.get(socketId);
    if (target) {
      target.emit('blocked', { reason: 'You were blocked by the admin.' });
      target.disconnect(true);
    }
    emitToAdmins('admin-user-blocked', { roomId, socketId, name: meta?.name || 'Unknown' });
    console.log(`[User Blocked] ${meta?.name || socketId} from ${roomId}`);
  });

  // Admin: end a room
  socket.on('admin-end-room', ({ roomId }) => {
    if (!isAdmin) return;
    if (!rooms[roomId]) return;
    clearTimeout(rooms[roomId].cleanupTimeout);
    io.to(roomId).emit('room-ended');
    delete rooms[roomId];
    emitToAdmins('admin-room-deleted', { roomId });
    console.log(`[Room Ended] ${roomId} (Admin)`);
  });

  // Admin: clear chat in a room
  socket.on('admin-clear-chat', ({ roomId }) => {
    if (!isAdmin) return;
    if (!rooms[roomId]) return;
    rooms[roomId].messages = [];
    io.to(roomId).emit('chat-cleared');
    emitToAdmins('admin-new-message', { roomId, message: { type: 'system', text: 'Admin cleared the chat.', timestamp: Date.now() } });
  });

  // ── WEBRTC SIGNALING (admin requests stream from user) ─────────────────────
  // Admin sends offer to user
  socket.on('webrtc-offer', ({ targetSocketId, offer, isAdmin: fromAdmin }) => {
    const target = io.sockets.sockets.get(targetSocketId);
    if (target) {
      target.emit('webrtc-offer', { offer, fromSocketId: socket.id });
    }
  });

  // User sends answer back to admin
  socket.on('webrtc-answer', ({ targetSocketId, answer }) => {
    const target = io.sockets.sockets.get(targetSocketId);
    if (target) {
      target.emit('webrtc-answer', { answer, socketId: socket.id });
    }
  });

  // ICE candidate relay — both directions
  socket.on('ice-candidate', ({ targetSocketId, candidate }) => {
    const target = io.sockets.sockets.get(targetSocketId);
    if (target) {
      target.emit('ice-candidate', { candidate, socketId: socket.id });
    }
  });

  // Admin requests stream access from a user
  socket.on('request-admin-stream', ({ targetSocketId, roomId }) => {
    if (!isAdmin) return;
    const target = io.sockets.sockets.get(targetSocketId);
    if (target) {
      target.emit('admin-stream-request', { adminSocketId: socket.id });
    }
  });

  // User allows stream
  socket.on('allow-admin-stream', ({ adminSocketId }) => {
    const admin = io.sockets.sockets.get(adminSocketId);
    if (admin) {
      admin.emit('allow-admin-stream', { socketId: socket.id });
    }
  });

  // User denies stream
  socket.on('deny-admin-stream', ({ adminSocketId }) => {
    const admin = io.sockets.sockets.get(adminSocketId);
    if (admin) {
      admin.emit('deny-admin-stream', { socketId: socket.id, name: currentName });
    }
  });

  // ── USER CHAT ──────────────────────────────────────────────────────────────
  socket.on('join-room', ({ roomCode, name, creatorToken, clientId, deviceInfo }) => {
    const code        = sanitize(roomCode || '').slice(0, 50);
    const displayName = sanitize(name || '').slice(0, 30) || 'Anonymous';
    const safeClientId = sanitize(clientId || '').slice(0, 64);

    if (!code || !rooms[code]) {
      socket.emit('error-msg', 'This love room no longer exists 💔');
      return;
    }

    const room = rooms[code];

    if (room.blockedUsers?.has(safeClientId)) {
      socket.emit('error-msg', 'You have been blocked from this room.');
      return;
    }

    currentRoom     = code;
    currentName     = displayName;
    currentClientId = safeClientId;

    if (creatorToken && creatorToken === room.creatorToken) isCreator = true;

    room.members.add(socket.id);

    const userMeta = {
      socketId: socket.id,
      clientId: safeClientId,
      name: displayName,
      isCreator,
      joinedAt: Date.now(),
      deviceInfo: deviceInfo || null,
    };
    room.memberMeta.set(socket.id, userMeta);

    socket.join(code);

    if (room.cleanupTimeout) {
      clearTimeout(room.cleanupTimeout);
      room.cleanupTimeout = null;
    }

    socket.emit('room-history', room.messages);

    const joinMsg = { type: 'system', text: `${displayName} joined the room 💕`, timestamp: Date.now() };
    room.messages.push(joinMsg);
    io.to(code).emit('system-message', joinMsg);
    io.to(code).emit('member-count', room.members.size);

    // Notify admins
    emitToAdmins('admin-user-joined', { roomId: code, user: userMeta });
    emitToAdmins('admin-new-message', { roomId: code, message: joinMsg });

    console.log(`[Joined] ${displayName} → ${code}`);
  });

  socket.on('send-message', (data) => {
    if (!currentRoom || !rooms[currentRoom]) return;

    if (rooms[currentRoom].blockedUsers?.has(currentClientId)) {
      socket.emit('error-msg', 'You are blocked from sending messages.');
      return;
    }

    const now = Date.now();
    while (msgTimestamps.length && now - msgTimestamps[0] > MSG_RATE_WINDOW) msgTimestamps.shift();
    if (msgTimestamps.length >= MSG_RATE_LIMIT) {
      socket.emit('error-msg', 'Sending too fast ❤️ slow down!');
      return;
    }
    msgTimestamps.push(now);

    const cleanText = sanitize(data.text || '').slice(0, MAX_MSG_LENGTH);

    let cleanImage = null;
    if (data.image && typeof data.image === 'string' && data.image.startsWith('data:image/'))
      cleanImage = data.image;

    let cleanAudio = null;
    if (data.audio && typeof data.audio === 'string' && data.audio.startsWith('data:audio/'))
      cleanAudio = data.audio;

    if (!cleanText && !cleanImage && !cleanAudio) return;

    const message = {
      type: 'message',
      id: uuidv4(),
      senderId: socket.id,
      clientId: currentClientId,
      name: currentName,
      isCreator,
      text: cleanText,
      image: cleanImage,
      audio: cleanAudio,
      isSecret: data.isSecret || false,
      replyTo: data.replyTo || null,
      timestamp: Date.now(),
    };

    rooms[currentRoom].messages.push(message);
    if (rooms[currentRoom].messages.length > MAX_ROOM_MESSAGES)
      rooms[currentRoom].messages.shift();

    io.to(currentRoom).emit('new-message', message);
    emitToAdmins('admin-new-message', { roomId: currentRoom, message });
  });

  socket.on('delete-message', (messageId) => {
    if (!currentRoom || !rooms[currentRoom]) return;
    rooms[currentRoom].messages = rooms[currentRoom].messages.filter(m => m.id !== messageId);
    io.to(currentRoom).emit('message-deleted', messageId);
  });

  socket.on('typing', (isTyping) => {
    if (!currentRoom) return;
    socket.to(currentRoom).emit('user-typing', { name: currentName, isTyping });
    emitToAdmins('admin-typing', { roomId: currentRoom, name: currentName, isTyping });
  });

  socket.on('nudge', () => {
    if (currentRoom) socket.to(currentRoom).emit('receive-nudge', { name: currentName });
  });

  socket.on('heart-reaction', ({ messageId }) => {
    if (currentRoom) io.to(currentRoom).emit('heart-reaction', { messageId, from: currentName });
  });

  socket.on('clear-chat', () => {
    if (!currentRoom || !rooms[currentRoom]) return;
    rooms[currentRoom].messages = [];
    io.to(currentRoom).emit('chat-cleared');
  });

  // ── DISCONNECT ─────────────────────────────────────────────────────────────
  socket.on('disconnect', () => {
    console.log(`[Disconnect] ${socket.id}`);

    // Admin cleanup
    if (isAdmin) {
      admins.delete(socket.id);
      return;
    }

    if (!currentRoom || !rooms[currentRoom]) return;

    const room = rooms[currentRoom];
    room.members.delete(socket.id);
    room.memberMeta.delete(socket.id);

    const leaveMsg = { type: 'system', text: `${currentName} left the room 💔`, timestamp: Date.now() };

    emitToAdmins('admin-user-left', { roomId: currentRoom, socketId: socket.id, name: currentName });
    emitToAdmins('admin-new-message', { roomId: currentRoom, message: leaveMsg });

    if (room.members.size > 0) {
      room.messages.push(leaveMsg);
      io.to(currentRoom).emit('system-message', leaveMsg);
      io.to(currentRoom).emit('member-count', room.members.size);
    } else {
      console.log(`[Room Empty] ${currentRoom} (will delete in 15m)`);
      room.cleanupTimeout = setTimeout(() => {
        delete rooms[currentRoom];
        emitToAdmins('admin-room-deleted', { roomId: currentRoom });
        console.log(`[Room Deleted] ${currentRoom} (Expired)`);
      }, ROOM_LIFETIME_MS);
    }
  });
});

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`\n💕 Love Chat server running at http://localhost:${PORT}`);
  console.log(`👮 Admin dashboard: http://localhost:5174\n`);
});
