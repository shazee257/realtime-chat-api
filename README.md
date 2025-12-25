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
| **Messages broadcast to all servers** | Redis Pub/Sub (or Socket.io Redis Adapter) broadcasts to all servers |
| **No missing messages** | Message history stored in Redis sorted set |
| **Guaranteed delivery** | New users receive message history on join |
| **Reliable Connectivity** | Socket.io handles reconnections and heartbeats automatically |

## 📁 Project Structure

```
src/
├── server.ts                    # Main entry point + Express app
└── services/
    ├── redis-pubsub.ts          # Redis Pub/Sub service (cross-server messaging)
    └── websocket-handler.ts     # Socket.io handler (client management)
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
1. User A (connected to Server 1) sends a message via socket.emit('message')
           │
           ▼
2. Server 1 receives the Socket.io message
           │
           ▼
3. Server 1 does TWO things:
   a) Stores message in Redis (for history/guaranteed delivery)
   b) Publishes message to Redis Pub/Sub channel
           │
           ▼
4. Redis broadcasts to ALL subscribed servers
           │
           ├──► Server 1 receives ──► io.emit('message') to local clients
           ├──► Server 2 receives ──► io.emit('message') to local clients
           └──► Server 3 receives ──► io.emit('message') to local clients
           │
           ▼
5. ALL users across ALL servers see the message!
```

### Why Socket.io?

Compared to raw WebSockets (`ws`), Socket.io provides:
1. **Automatic Reconnection**: If the server goes down, clients reconnect automatically.
2. **Built-in Heartbeats**: No need to manually implement ping/pong.
3. **Event-based API**: Cleaner code with `socket.on` and `socket.emit`.
4. **Binary Support**: Native support for Buffers and typed arrays.

## 🔧 Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3001` | Server port |
| `SERVER_ID` | `server-{PORT}` | Unique server identifier |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | - | Redis password (optional) |

## 📡 API Reference

### Socket.io Events

**Client → Server:**

- `join`: `{ username: 'Alice' }` - Join the chat
- `message`: `{ content: 'Hello!' }` - Send a message

**Server → Client:**

- `join_success`: `{ userId, username, serverId, message }` - Join confirmation
- `history`: `{ messages: [...], serverId }` - Message history on join
- `message`: `{ id, userId, username, content, timestamp, serverId }` - New message
- `join`: `{ userId, username, serverId, timestamp }` - Other user joined
- `leave`: `{ userId, username, serverId, timestamp }` - Other user left
- `error_msg`: `{ message }` - Error notification

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
