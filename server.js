const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
});

app.use(express.static(path.join(__dirname, 'public')));

// Store active streams: key -> { broadcaster: socketId, viewers: Set<socketId> }
const streams = new Map();

io.on('connection', (socket) => {
  console.log(`[+] Connected: ${socket.id}`);

  // Broadcaster creates a new stream
  socket.on('create-stream', (callback) => {
    const streamKey = uuidv4().split('-')[0]; // short key like "a3f2b1c9"
    streams.set(streamKey, {
      broadcaster: socket.id,
      viewers: new Set()
    });
    socket.streamKey = streamKey;
    socket.role = 'broadcaster';
    socket.join(streamKey);
    console.log(`[STREAM] Created: ${streamKey} by ${socket.id}`);
    callback({ streamKey });
  });

  // Viewer joins an existing stream
  socket.on('join-stream', (streamKey, callback) => {
    const stream = streams.get(streamKey);
    if (!stream) {
      callback({ error: 'Stream not found. Check the key and try again.' });
      return;
    }
    stream.viewers.add(socket.id);
    socket.streamKey = streamKey;
    socket.role = 'viewer';
    socket.join(streamKey);
    console.log(`[VIEW] ${socket.id} joined stream ${streamKey}`);
    // Notify broadcaster a new viewer wants to connect
    io.to(stream.broadcaster).emit('viewer-joined', socket.id);
    callback({ success: true });
  });

  // WebRTC signaling: offer from broadcaster to viewer
  socket.on('offer', (viewerId, offer) => {
    io.to(viewerId).emit('offer', socket.id, offer);
  });

  // WebRTC signaling: answer from viewer to broadcaster
  socket.on('answer', (broadcasterId, answer) => {
    io.to(broadcasterId).emit('answer', socket.id, answer);
  });

  // WebRTC signaling: ICE candidates
  socket.on('ice-candidate', (targetId, candidate) => {
    io.to(targetId).emit('ice-candidate', socket.id, candidate);
  });

  socket.on('disconnect', () => {
    console.log(`[-] Disconnected: ${socket.id}`);
    if (socket.role === 'broadcaster' && socket.streamKey) {
      const stream = streams.get(socket.streamKey);
      if (stream) {
        // Notify all viewers the stream ended
        io.to(socket.streamKey).emit('stream-ended');
        streams.delete(socket.streamKey);
        console.log(`[STREAM] Ended: ${socket.streamKey}`);
      }
    } else if (socket.role === 'viewer' && socket.streamKey) {
      const stream = streams.get(socket.streamKey);
      if (stream) {
        stream.viewers.delete(socket.id);
        io.to(stream.broadcaster).emit('viewer-left', socket.id);
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n  ========================================`);
  console.log(`   TEKA Screen Share`);
  console.log(`   Running on http://localhost:${PORT}`);
  console.log(`  ========================================\n`);
});
