// REVEAL — Node.js + Socket.io Backend
// Deploy: Render.com (Free tier)

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

app.use(cors());
app.use(express.json());

// ── State ──
const waitingQueue = [];        // [{socketId, mask, sparkAnswer, joinedAt}]
const activeRooms  = new Map(); // roomId → {users:[socketId,socketId], revealed:false, createdAt}
const userRoom     = new Map(); // socketId → roomId
const userMeta     = new Map(); // socketId → {mask, sparkAnswer}

// ── Health check ──
app.get('/', (_, res) => res.send('REVEAL server running ✓'));
app.get('/status', (_, res) => res.json({
  waiting: waitingQueue.length,
  activeRooms: activeRooms.size,
  connectedUsers: io.engine.clientsCount,
}));

// ── Socket.io ──
io.on('connection', (socket) => {
  console.log(`[+] ${socket.id}`);

  // 1. User joins queue
  socket.on('join_queue', ({ mask, sparkAnswer }) => {
    userMeta.set(socket.id, { mask, sparkAnswer });

    // Remove if already in queue (reconnect case)
    const idx = waitingQueue.findIndex(u => u.socketId === socket.id);
    if (idx !== -1) waitingQueue.splice(idx, 1);

    // Try to match with someone waiting
    if (waitingQueue.length > 0) {
      const partner = waitingQueue.shift();

      const roomId = `room_${Date.now()}_${Math.random().toString(36).slice(2,7)}`;
      activeRooms.set(roomId, {
        users: [socket.id, partner.socketId],
        revealVotes: {},
        createdAt: Date.now(),
      });
      userRoom.set(socket.id, roomId);
      userRoom.set(partner.socketId, roomId);

      socket.join(roomId);
      io.sockets.sockets.get(partner.socketId)?.join(roomId);

      // Notify both
      const myMeta    = userMeta.get(socket.id);
      const theirMeta = userMeta.get(partner.socketId);

      socket.emit('matched', {
        roomId,
        partnerMask: theirMeta.mask,
        partnerSpark: theirMeta.sparkAnswer,
      });
      io.to(partner.socketId).emit('matched', {
        roomId,
        partnerMask: myMeta.mask,
        partnerSpark: myMeta.sparkAnswer,
      });

      console.log(`[match] ${socket.id} ↔ ${partner.socketId} → ${roomId}`);
    } else {
      waitingQueue.push({ socketId: socket.id, mask, sparkAnswer, joinedAt: Date.now() });
      socket.emit('waiting');
      console.log(`[queue] ${socket.id} waiting (queue size: ${waitingQueue.length})`);
    }
  });

  // 2. Chat message
  socket.on('send_message', ({ roomId, text }) => {
    if (!roomId || !text?.trim()) return;
    const room = activeRooms.get(roomId);
    if (!room?.users.includes(socket.id)) return;

    socket.to(roomId).emit('receive_message', {
      text: text.trim(),
      ts: Date.now(),
    });
  });

  // 3. Typing indicator
  socket.on('typing', ({ roomId, isTyping }) => {
    socket.to(roomId).emit('partner_typing', { isTyping });
  });

  // 4. Reveal vote
  socket.on('reveal_vote', ({ roomId, accept }) => {
    const room = activeRooms.get(roomId);
    if (!room) return;

    room.revealVotes[socket.id] = accept;

    // Notify partner of vote (without revealing result yet)
    socket.to(roomId).emit('partner_voted');

    const users = room.users;
    const bothVoted = users.every(uid => uid in room.revealVotes);

    if (bothVoted) {
      const bothAccepted = users.every(uid => room.revealVotes[uid] === true);
      if (bothAccepted) {
        // Send each user the other's meta
        const [a, b] = users;
        const metaA = userMeta.get(a);
        const metaB = userMeta.get(b);
        io.to(a).emit('reveal_result', { accepted: true, partnerMeta: metaB });
        io.to(b).emit('reveal_result', { accepted: true, partnerMeta: metaA });
        room.revealed = true;
      } else {
        io.to(roomId).emit('reveal_result', { accepted: false });
      }
    }
  });

  // 5. Leave room
  socket.on('leave_room', ({ roomId }) => {
    cleanupUser(socket, roomId);
  });

  // 6. Disconnect
  socket.on('disconnect', () => {
    console.log(`[-] ${socket.id}`);
    const roomId = userRoom.get(socket.id);
    if (roomId) {
      socket.to(roomId).emit('partner_left');
      cleanupUser(socket, roomId);
    }
    // Remove from queue if waiting
    const qi = waitingQueue.findIndex(u => u.socketId === socket.id);
    if (qi !== -1) waitingQueue.splice(qi, 1);
    userMeta.delete(socket.id);
  });
});

function cleanupUser(socket, roomId) {
  const room = activeRooms.get(roomId);
  if (room) {
    // Remove both users from room tracking
    room.users.forEach(uid => userRoom.delete(uid));
    activeRooms.delete(roomId);
  }
  socket.leave(roomId);
  userRoom.delete(socket.id);
}

// Cleanup stale rooms every 10 min
setInterval(() => {
  const now = Date.now();
  for (const [roomId, room] of activeRooms) {
    if (now - room.createdAt > 60 * 60 * 1000) { // 1 hour
      activeRooms.delete(roomId);
      console.log(`[cleanup] stale room ${roomId}`);
    }
  }
}, 10 * 60 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`REVEAL server → port ${PORT}`));
