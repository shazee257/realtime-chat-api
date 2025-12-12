# Realtime Chat with Horizontal WebSocket Scaling

A simple, well-documented implementation of horizontally scalable WebSockets using **Redis Pub/Sub**.

## 🏗️ Architecture

```
                    ┌─────────────────────────────────────────┐
                    │            LOAD BALANCER                 │
                    │         (nginx, haproxy, etc.)           │
                    └───────────────────┬─────────────────────┘
                                        │
            ┌───────────────────────────┼───────────────────────────┐
            ▼                           ▼                           ▼
      ┌──────────┐               ┌──────────┐               ┌──────────┐
      │ Server 1 │               │ Server 2 │               │ Server 3 │
      │  :3001   │               │  :3002   │               │  :3003   │
      │          │               │          │               │          │
      │ Clients: │               │ Clients: │               │ Clients: │
      │  A, B    │               │  C, D    │               │  E, F    │
      └────┬─────┘               └────┬─────┘               └────┬─────┘
           │                          │                          │
           └──────────────────────────┼──────────────────────────┘
                                      │
                                      ▼
                              ┌──────────────┐
                              │    REDIS     │
                              │   Pub/Sub    │
                              │   + Store    │
                              └──────────────┘
```

## 🎯 Key Features

| Feature | Implementation |
|---------|---------------|
| **Any user can connect to any server** | Users connect via load balancer or directly to any server |
| **Messages broadcast to all servers** | Redis Pub/Sub broadcasts to all subscribed servers |
| **No missing messages** | Message history stored in Redis sorted set |
| **Guaranteed delivery** | New users receive message history on join |
| **Lower latency** | Direct WebSocket + in-memory Redis = minimal latency |

## 📁 Project Structure

```
src/
├── server.ts                    # Main entry point + Express app
└── services/
    ├── redis-pubsub.ts          # Redis Pub/Sub service (cross-server messaging)
    └── websocket-handler.ts     # WebSocket handler (client management)
```

## 🚀 Quick Start

### Prerequisites

- Node.js 18+
- Redis server running locally (or remote)

### Installation

```bash
# Install dependencies
npm install

# Start Redis (if using Docker)
docker run -d -p 6379:6379 redis:latest

# Or install Redis locally
# macOS: brew install redis && redis-server
# Ubuntu: sudo apt install redis-server && sudo systemctl start redis
```

### Running Multiple Servers

Open **3 terminals** and run:

```bash
# Terminal 1
npm run dev:server1

# Terminal 2
npm run dev:server2

# Terminal 3
npm run dev:server3
```

### Testing

1. Open `http://localhost:3001` in one browser tab
2. Open `http://localhost:3002` in another tab
3. Open `http://localhost:3003` in a third tab
4. Join with different usernames and send messages
5. **See messages appear across ALL servers!**

## 📚 How It Works

### Message Flow

```
1. User A (connected to Server 1) sends a message
           │
           ▼
2. Server 1 receives the WebSocket message
           │
           ▼
3. Server 1 does TWO things:
   a) Stores message in Redis (for history/guaranteed delivery)
   b) Publishes message to Redis Pub/Sub channel
           │
           ▼
4. Redis broadcasts to ALL subscribed servers
           │
           ├──► Server 1 receives ──► Broadcasts to local clients (A, B)
           ├──► Server 2 receives ──► Broadcasts to local clients (C, D)
           └──► Server 3 receives ──► Broadcasts to local clients (E, F)
           │
           ▼
5. ALL users across ALL servers see the message!
```

### Why Redis Pub/Sub?

Without Redis, each server only knows about its own connected clients:

```
❌ WITHOUT REDIS:
   Server 1 clients can only see Server 1 messages
   Server 2 clients can only see Server 2 messages
   (Messages are isolated!)

✅ WITH REDIS:
   All servers subscribe to the same Redis channel
   When any server publishes, ALL servers receive
   Each server broadcasts to its local clients
   (Messages are synchronized!)
```

### Deduplication

Since the publishing server also receives its own message back from Redis, we track processed message IDs:

```typescript
private processedMessages: Set<string> = new Set();

// When receiving from Redis:
if (this.processedMessages.has(message.id)) {
  return; // Skip - already processed
}
this.processedMessages.add(message.id);
```

### Guaranteed Delivery

New users don't miss messages because:

1. All messages are stored in Redis (sorted set with timestamp)
2. When a user joins, they receive the last 50 messages
3. The history is fetched from Redis, not from the server's memory

```typescript
// On user join:
const history = await redis.getRecentMessages(50);
client.send(JSON.stringify({ type: 'history', payload: { messages: history } }));
```

## 🔧 Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3001` | Server port |
| `SERVER_ID` | `server-{PORT}` | Unique server identifier |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | - | Redis password (optional) |

### Example with Remote Redis

```bash
PORT=3001 \
SERVER_ID=prod-server-1 \
REDIS_HOST=redis.example.com \
REDIS_PORT=6379 \
REDIS_PASSWORD=secret \
npm run start
```

## 📡 API Reference

### WebSocket Messages

**Client → Server:**

```typescript
// Join the chat
{ type: 'join', payload: { username: 'Alice' } }

// Send a message
{ type: 'message', payload: { content: 'Hello!' } }

// Ping (keepalive)
{ type: 'ping' }
```

**Server → Client:**

```typescript
// Join confirmation
{ type: 'join', payload: { userId, username, serverId, message } }

// Message history on join
{ type: 'history', payload: { messages: [...], serverId } }

// New message
{ type: 'message', payload: { id, userId, username, content, timestamp, serverId } }

// User left
{ type: 'leave', payload: { userId, username, serverId, timestamp } }

// Pong response
{ type: 'pong' }
```

### REST Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Web client interface |
| `GET /health` | Health check |
| `GET /info` | Server information |
| `GET /messages` | Get recent messages |

## 🌐 Production Deployment

### Load Balancer (nginx example)

```nginx
upstream chat_servers {
    ip_hash;  # Sticky sessions for WebSocket
    server 127.0.0.1:3001;
    server 127.0.0.1:3002;
    server 127.0.0.1:3003;
}

server {
    listen 80;
    
    location / {
        proxy_pass http://chat_servers;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
    }
}
```

### Docker Compose

```yaml
version: '3.8'
services:
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"

  chat-1:
    build: .
    environment:
      - PORT=3001
      - SERVER_ID=chat-1
      - REDIS_HOST=redis
    ports:
      - "3001:3001"

  chat-2:
    build: .
    environment:
      - PORT=3002
      - SERVER_ID=chat-2
      - REDIS_HOST=redis
    ports:
      - "3002:3002"

  chat-3:
    build: .
    environment:
      - PORT=3003
      - SERVER_ID=chat-3
      - REDIS_HOST=redis
    ports:
      - "3003:3003"
```

## 📝 License

MIT
