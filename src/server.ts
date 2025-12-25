/**
 * ============================================
 * MAIN SERVER ENTRY POINT
 * ============================================
 * 
 * This file bootstraps the chat server with:
 * - Express HTTP server
 * - WebSocket server (attached to HTTP)
 * - Redis Pub/Sub connection
 * 
 * HORIZONTAL SCALING ARCHITECTURE:
 * 
 *   ┌─────────────────────────────────────────────────────────────┐
 *   │                     LOAD BALANCER                            │
 *   │                  (nginx, haproxy, etc.)                      │
 *   └───────────────────────┬─────────────────────────────────────┘
 *                           │
 *       ┌───────────────────┼───────────────────┐
 *       ▼                   ▼                   ▼
 *   ┌────────┐         ┌────────┐         ┌────────┐
 *   │Server 1│         │Server 2│         │Server 3│
 *   │:3001   │         │:3002   │         │:3003   │
 *   └────┬───┘         └────┬───┘         └────┬───┘
 *        │                  │                  │
 *        └──────────────────┼──────────────────┘
 *                           │
 *                           ▼
 *                    ┌────────────┐
 *                    │   REDIS    │
 *                    │  Pub/Sub   │
 *                    └────────────┘
 * 
 * HOW TO RUN MULTIPLE SERVERS:
 * 
 *   Terminal 1: PORT=3001 SERVER_ID=server-1 npm run dev
 *   Terminal 2: PORT=3002 SERVER_ID=server-2 npm run dev
 *   Terminal 3: PORT=3003 SERVER_ID=server-3 npm run dev
 * 
 * Or use the convenience scripts:
 * 
 *   npm run dev:server1
 *   npm run dev:server2
 *   npm run dev:server3
 */

import express from 'express';
import cors from 'cors';
import { createServer } from 'http';
import { RedisPubSub } from './services/redis-pubsub';
import { WebSocketHandler } from './services/websocket-handler';

// Server configuration from environment
const PORT = parseInt(process.env.PORT || '3001');
const SERVER_ID = process.env.SERVER_ID || `server-${PORT}`;

async function main() {
  console.log('\n========================================');
  console.log('   REALTIME CHAT SERVER');
  console.log('   Horizontal WebSocket Scaling Demo');
  console.log('========================================\n');
  console.log(`Server ID: ${SERVER_ID}`);
  console.log(`Port: ${PORT}`);
  console.log('');

  // 1. Create Express app
  const app = express();
  app.use(cors());
  app.use(express.json());

  // 2. Create HTTP server (needed for WebSocket attachment)
  const server = createServer(app);

  // 3. Initialize Redis Pub/Sub
  const redis = new RedisPubSub(SERVER_ID);
  await redis.subscribe();
  console.log('[Server] Redis Pub/Sub initialized');

  // 4. Initialize Socket.io handler
  const socketHandler = new WebSocketHandler(server, redis);
  console.log('[Server] Socket.io handler initialized');

  // ====================================
  // HTTP ENDPOINTS
  // ====================================

  // Health check endpoint
  app.get('/health', (req, res) => {
    res.json({
      status: 'healthy',
      serverId: SERVER_ID,
      port: PORT,
      timestamp: new Date().toISOString(),
      connectedClients: socketHandler.getClientCount(),
      redis: redis.isReady() ? 'connected' : 'disconnected',
    });
  });

  // Get server info
  app.get('/info', (req, res) => {
    res.json({
      serverId: SERVER_ID,
      port: PORT,
      connectedClients: socketHandler.getClientCount(),
      uptime: process.uptime(),
    });
  });

  // Get recent messages (REST fallback)
  app.get('/messages', async (req, res) => {
    try {
      const messages = await redis.getRecentMessages(50);
      res.json({
        messages,
        count: messages.length,
        serverId: SERVER_ID,
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch messages' });
    }
  });

  // Serve simple test client
  app.get('/', (req, res) => {
    res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <title>Chat Server - ${SERVER_ID}</title>
        <script src="/socket.io/socket.io.js"></script>
        <style>
          * { box-sizing: border-box; font-family: 'Segoe UI', sans-serif; }
          body { 
            margin: 0; 
            padding: 20px; 
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #fff;
            min-height: 100vh;
          }
          .container { max-width: 800px; margin: 0 auto; }
          .header { 
            text-align: center; 
            padding: 20px;
            background: rgba(255,255,255,0.1);
            border-radius: 12px;
            margin-bottom: 20px;
          }
          .header h1 { margin: 0; color: #00d4ff; }
          .header .server-id { color: #ff6b6b; font-size: 14px; }
          #messages { 
            height: 400px; 
            overflow-y: auto; 
            border: 1px solid rgba(255,255,255,0.1); 
            padding: 15px; 
            margin-bottom: 15px;
            background: rgba(0,0,0,0.3);
            border-radius: 12px;
          }
          .message { 
            margin: 10px 0; 
            padding: 12px 15px;
            background: rgba(255,255,255,0.05);
            border-radius: 8px;
            border-left: 3px solid #00d4ff;
          }
          .message .meta { 
            font-size: 12px; 
            color: #888; 
            margin-bottom: 5px;
          }
          .message .server { color: #ff6b6b; }
          .message.system { 
            border-left-color: #ffd93d;
            background: rgba(255, 217, 61, 0.1);
          }
          .input-area { display: flex; gap: 10px; }
          input { 
            flex: 1; 
            padding: 15px; 
            border: none;
            border-radius: 8px;
            background: rgba(255,255,255,0.1);
            color: #fff;
            font-size: 16px;
          }
          input::placeholder { color: rgba(255,255,255,0.4); }
          button { 
            padding: 15px 30px; 
            background: linear-gradient(135deg, #00d4ff, #0099cc);
            color: #fff;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-weight: bold;
            font-size: 16px;
          }
          button:hover { opacity: 0.9; }
          .status { 
            text-align: center; 
            padding: 10px;
            color: #888;
            font-size: 14px;
          }
          .status.connected { color: #4ade80; }
          .status.disconnected { color: #f87171; }
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h1>🚀 Realtime Chat</h1>
            <div class="server-id">Connected to: ${SERVER_ID} (Port ${PORT})</div>
          </div>
          <div id="status" class="status disconnected">Connecting...</div>
          <div id="messages"></div>
          <div class="input-area">
            <input type="text" id="username" placeholder="Enter username..." style="flex: 0.3;">
            <input type="text" id="message" placeholder="Type a message..." disabled>
            <button id="joinBtn" onclick="join()">Join</button>
            <button id="sendBtn" onclick="send()" style="display:none;">Send</button>
          </div>
        </div>
        <script>
          let socket;
          let joined = false;

          function connect() {
            socket = io({
                reconnection: true,
                reconnectionDelay: 1000,
                reconnectionDelayMax: 5000,
                reconnectionAttempts: Infinity
            });
            
            socket.on('connect', () => {
              document.getElementById('status').textContent = 'Connected to ${SERVER_ID}';
              document.getElementById('status').className = 'status connected';
            });
            
            socket.on('history', (data) => {
              data.messages.forEach(msg => {
                addMessage(msg.content, msg.username, msg.serverId);
              });
            });

            socket.on('join_success', (data) => {
              addMessage('🎉 ' + data.message, 'System', '', true);
            });

            socket.on('message', (data) => {
              addMessage(data.content, data.username, data.serverId);
            });

            socket.on('join', (data) => {
              addMessage('✨ ' + data.username + ' joined the chat', 'System', data.serverId, true);
            });

            socket.on('leave', (data) => {
              addMessage('👋 ' + data.username + ' left', 'System', data.serverId, true);
            });
            
            socket.on('disconnect', () => {
              document.getElementById('status').textContent = 'Disconnected - Reconnecting...';
              document.getElementById('status').className = 'status disconnected';
            });

            socket.on('error_msg', (data) => {
                alert(data.message);
            });
          }

          function addMessage(content, username, serverId, isSystem = false) {
            const messages = document.getElementById('messages');
            const div = document.createElement('div');
            div.className = 'message' + (isSystem ? ' system' : '');
            div.innerHTML = 
              '<div class="meta"><strong>' + username + '</strong>' + 
              (serverId ? ' <span class="server">via ' + serverId + '</span>' : '') +
              '</div>' +
              '<div>' + content + '</div>';
            messages.appendChild(div);
            messages.scrollTop = messages.scrollHeight;
          }

          function join() {
            const username = document.getElementById('username').value.trim();
            if (!username) return alert('Please enter a username');
            
            socket.emit('join', { username });
            
            document.getElementById('username').disabled = true;
            document.getElementById('message').disabled = false;
            document.getElementById('joinBtn').style.display = 'none';
            document.getElementById('sendBtn').style.display = 'block';
            document.getElementById('message').focus();
            joined = true;
          }

          function send() {
            const input = document.getElementById('message');
            const content = input.value.trim();
            if (!content) return;
            
            socket.emit('message', { content });
            
            input.value = '';
          }

          document.getElementById('message').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') send();
          });

          document.getElementById('username').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') join();
          });

          connect();
        </script>
      </body>
      </html>
    `);
  });

  // ====================================
  // START SERVER
  // ====================================

  server.listen(PORT, () => {
    console.log('');
    console.log('========================================');
    console.log(`✅ Server ${SERVER_ID} running on port ${PORT}`);
    console.log('========================================');
    console.log('');
    console.log('📡 Socket.io: http://localhost:' + PORT);
    console.log('🌐 Web Client: http://localhost:' + PORT);
    console.log('❤️  Health: http://localhost:' + PORT + '/health');
    console.log('');
    console.log('Try running multiple servers:');
    console.log('  npm run dev:server1  (port 3001)');
    console.log('  npm run dev:server2  (port 3002)');
    console.log('  npm run dev:server3  (port 3003)');
    console.log('');
  });

  // ====================================
  // GRACEFUL SHUTDOWN
  // ====================================

  process.on('SIGTERM', async () => {
    console.log('\n[Server] Shutting down gracefully...');
    await socketHandler.close();
    await redis.disconnect();
    server.close(() => {
      console.log('[Server] Goodbye!');
      process.exit(0);
    });
  });

  process.on('SIGINT', async () => {
    console.log('\n[Server] Shutting down gracefully...');
    await socketHandler.close();
    await redis.disconnect();
    server.close(() => {
      console.log('[Server] Goodbye!');
      process.exit(0);
    });
  });
}

// Run the server
main().catch((error) => {
  console.error('[Server] Failed to start:', error);
  process.exit(1);
});
