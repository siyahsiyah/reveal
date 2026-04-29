const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

app.get('/', (req, res) => res.send('Reveal Backend ✓'));

// ── QUEUE & ROOMS ──
const queue = []; // { socketId, mask, sparkAnswer }
const rooms = {}; // roomId → { players: [sid1, sid2], votes: {} }

function makeRoomId() {
  return Math.random().toString(36).slice(2, 10);
}

io.on('connection', (socket) => {
  console.log('connect:', socket.id);

  // ── JOIN QUEUE ──
  socket.on('join_queue', ({ mask, sparkAnswer }) => {
    // Aynı kişi tekrar join_queue gönderirse öncekini temizle
    const idx = queue.findIndex(q => q.socketId === socket.id);
    if (idx !== -1) queue.splice(idx, 1);

    if (queue.length > 0) {
      // Eşleşme var
      const partner = queue.shift();
      const roomId = makeRoomId();

      rooms[roomId] = {
        players: [socket.id, partner.socketId],
        votes: {}
      };

      socket.join(roomId);
      io.sockets.sockets.get(partner.socketId)?.join(roomId);

      // Her ikisine de matched gönder
      socket.emit('matched', {
        roomId,
        partnerMask: partner.mask,
        partnerSpark: partner.sparkAnswer
      });

      io.to(partner.socketId).emit('matched', {
        roomId,
        partnerMask: mask,
        partnerSpark: sparkAnswer
      });

    } else {
      // Kuyruğa ekle
      queue.push({ socketId: socket.id, mask, sparkAnswer });
      socket.emit('waiting');
    }
  });

  // ── SEND MESSAGE ──
  socket.on('send_message', ({ roomId, text }) => {
    if (!rooms[roomId]) return;
    socket.to(roomId).emit('receive_message', {
      text,
      ts: Date.now()
    });
  });

  // ── TYPING ──
  socket.on('typing', ({ roomId, isTyping }) => {
    if (!rooms[roomId]) return;
    socket.to(roomId).emit('partner_typing', { isTyping });
  });

  // ── REVEAL VOTE ──
  socket.on('reveal_vote', ({ roomId, accept }) => {
    const room = rooms[roomId];
    if (!room) return;

    room.votes[socket.id] = accept;

    // Karşı tarafa "partner_voted" gönder (kabul/ret bilgisi olmadan)
    socket.to(roomId).emit('partner_voted');

    const players = room.players;
    const allVoted = players.every(pid => pid in room.votes);

    if (allVoted) {
      const bothAccepted = players.every(pid => room.votes[pid] === true);
      players.forEach(pid => {
        io.to(pid).emit('reveal_result', {
          accepted: bothAccepted,
          partnerMeta: null // gerçek isim/foto için auth eklenebilir
        });
      });
      // Oyları sıfırla
      room.votes = {};
    }
  });

  // ── LEAVE ROOM ──
  socket.on('leave_room', ({ roomId }) => {
    leaveRoom(socket, roomId);
  });

  // ── DISCONNECT ──
  socket.on('disconnect', () => {
    console.log('disconnect:', socket.id);

    // Kuyruktan çıkar
    const qi = queue.findIndex(q => q.socketId === socket.id);
    if (qi !== -1) queue.splice(qi, 1);

    // Odadan çıkar ve partnere haber ver
    for (const roomId of Object.keys(rooms)) {
      if (rooms[roomId].players.includes(socket.id)) {
        leaveRoom(socket, roomId);
        break;
      }
    }
  });

  function leaveRoom(socket, roomId) {
    const room = rooms[roomId];
    if (!room) return;
    socket.to(roomId).emit('partner_left');
    socket.leave(roomId);
    delete rooms[roomId];
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Reveal backend port ${PORT}`));
