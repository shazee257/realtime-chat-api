/**
 * ============================================
 * WEBSOCKET HANDLER
 * ============================================
 * 
 * This module manages WebSocket connections for a single server instance.
 * 
 * KEY CONCEPTS:
 * 
 * 1. LOCAL CONNECTIONS: Each server tracks only its own connected clients
 * 2. MESSAGE DEDUPLICATION: We track processed message IDs to avoid duplicates
 * 3. REDIS INTEGRATION: Messages are published to Redis for cross-server delivery
 * 
 * MESSAGE FLOW (User sends a message):
 * 
 *   ┌─────────────────────────────────────────────────────────────────┐
 *   │                        MESSAGE FLOW                              │
 *   ├─────────────────────────────────────────────────────────────────┤
 *   │                                                                  │
 *   │  1. Client sends message via WebSocket                          │
 *   │           │                                                      │
 *   │           ▼                                                      │
 *   │  2. Server receives message                                      │
 *   │           │                                                      │
 *   │           ▼                                                      │
 *   │  3. Server publishes to Redis + stores in history                │
 *   │           │                                                      │
 *   │           ▼                                                      │
 *   │  4. Redis broadcasts to ALL subscribed servers                   │
 *   │           │                                                      │
 *   │           ├──► Server 1 ──► Broadcasts to local clients          │
 *   │           ├──► Server 2 ──► Broadcasts to local clients          │
 *   │           └──► Server 3 ──► Broadcasts to local clients          │
 *   │                                                                  │
 *   └─────────────────────────────────────────────────────────────────┘
 */

import { WebSocket, WebSocketServer } from 'ws';
import { v4 as uuidv4 } from 'uuid';
import { RedisPubSub, ChatMessage, CHANNELS } from './redis-pubsub';
import { Server } from 'http';

// Extended WebSocket type with user info
interface ExtendedWebSocket extends WebSocket {
    id: string;
    userId: string;
    username: string;
    isAlive: boolean;
}

// Message types for WebSocket communication
interface WSMessage {
    type: 'message' | 'join' | 'leave' | 'history' | 'ping' | 'pong';
    payload: any;
}

export class WebSocketHandler {
    private wss: WebSocketServer;
    private redis: RedisPubSub;
    private serverId: string;
    private clients: Map<string, ExtendedWebSocket> = new Map();
    private processedMessages: Set<string> = new Set(); // For deduplication
    private heartbeatInterval: NodeJS.Timeout | null = null;

    constructor(server: Server, redis: RedisPubSub) {
        this.redis = redis;
        this.serverId = redis.getServerId();

        // Create WebSocket server attached to HTTP server
        this.wss = new WebSocketServer({ server });

        this.setupWebSocketHandlers();
        this.setupRedisHandlers();
        this.startHeartbeat();

        console.log(`[WebSocket] Server initialized - ${this.serverId}`);
    }

    /**
     * Set up WebSocket connection handlers
     */
    private setupWebSocketHandlers(): void {
        this.wss.on('connection', async (ws: WebSocket) => {
            // Extend the WebSocket with custom properties
            const client = ws as ExtendedWebSocket;
            client.id = uuidv4();
            client.isAlive = true;

            console.log(`[WebSocket] New connection: ${client.id} on ${this.serverId}`);

            // Handle incoming messages from this client
            client.on('message', (data: Buffer) => {
                this.handleClientMessage(client, data);
            });

            // Handle client disconnect
            client.on('close', () => {
                this.handleClientDisconnect(client);
            });

            // Handle pong responses for heartbeat
            client.on('pong', () => {
                client.isAlive = true;
            });

            // Handle errors
            client.on('error', (error) => {
                console.error(`[WebSocket] Client error ${client.id}:`, error.message);
            });
        });
    }

    /**
     * Handle messages received from WebSocket clients
     */
    private async handleClientMessage(client: ExtendedWebSocket, data: Buffer): Promise<void> {
        try {
            const message: WSMessage = JSON.parse(data.toString());

            switch (message.type) {
                case 'join':
                    await this.handleJoin(client, message.payload);
                    break;

                case 'message':
                    await this.handleChatMessage(client, message.payload);
                    break;

                case 'ping':
                    // Respond to client ping
                    client.send(JSON.stringify({ type: 'pong' }));
                    break;

                default:
                    console.log(`[WebSocket] Unknown message type: ${message.type}`);
            }
        } catch (error) {
            console.error('[WebSocket] Failed to parse message:', error);
        }
    }

    /**
     * Handle user joining the chat
     */
    private async handleJoin(client: ExtendedWebSocket, payload: { username: string }): Promise<void> {
        client.userId = uuidv4();
        client.username = payload.username || `User-${client.id.slice(0, 4)}`;

        // Track this client
        this.clients.set(client.id, client);

        console.log(`[WebSocket] User joined: ${client.username} (${client.userId}) on ${this.serverId}`);

        // Send recent message history (GUARANTEED DELIVERY - no missed messages!)
        const history = await this.redis.getRecentMessages(50);
        client.send(JSON.stringify({
            type: 'history',
            payload: {
                messages: history,
                serverId: this.serverId,
            },
        }));

        // Notify all servers about new user
        await this.redis.publishUserEvent({
            type: 'join',
            userId: client.userId,
            username: client.username,
            serverId: this.serverId,
            timestamp: Date.now(),
        });

        // Send confirmation to the joining client
        client.send(JSON.stringify({
            type: 'join',
            payload: {
                userId: client.userId,
                username: client.username,
                serverId: this.serverId,
                message: `Welcome ${client.username}! You are connected to ${this.serverId}`,
            },
        }));
    }

    /**
     * Handle incoming chat message from a client
     * 
     * This is where the magic happens:
     * 1. Create message with unique ID
     * 2. Store in Redis for history
     * 3. Publish to Redis Pub/Sub
     * 4. All servers receive and broadcast to their clients
     */
    private async handleChatMessage(client: ExtendedWebSocket, payload: { content: string }): Promise<void> {
        if (!client.userId) {
            client.send(JSON.stringify({
                type: 'error',
                payload: { message: 'Please join first' },
            }));
            return;
        }

        // Create the message with a unique ID
        const chatMessage: ChatMessage = {
            id: uuidv4(),                    // Unique ID for deduplication
            userId: client.userId,
            username: client.username,
            content: payload.content,
            timestamp: Date.now(),
            serverId: this.serverId,         // Track which server received it
        };

        // Store message in Redis history (GUARANTEED DELIVERY)
        await this.redis.storeMessage(chatMessage);

        // Publish to Redis Pub/Sub (broadcasts to ALL servers)
        await this.redis.publishMessage(chatMessage);

        console.log(`[WebSocket] Message from ${client.username}: "${payload.content.slice(0, 50)}..."`);
    }

    /**
     * Handle client disconnection
     */
    private async handleClientDisconnect(client: ExtendedWebSocket): Promise<void> {
        if (client.userId) {
            // Notify all servers about user leaving
            await this.redis.publishUserEvent({
                type: 'leave',
                userId: client.userId,
                username: client.username,
                serverId: this.serverId,
                timestamp: Date.now(),
            });

            console.log(`[WebSocket] User left: ${client.username} (${client.userId}) from ${this.serverId}`);
        }

        // Remove from our tracked clients
        this.clients.delete(client.id);
    }

    /**
     * Set up handlers for Redis Pub/Sub messages
     * 
     * When Redis publishes a message, ALL servers receive it
     * Each server then broadcasts to its local WebSocket clients
     */
    private setupRedisHandlers(): void {
        // Handle chat messages from Redis
        this.redis.on(CHANNELS.CHAT_MESSAGES, (message: ChatMessage) => {
            // Deduplication: Skip if we've already processed this message
            if (this.processedMessages.has(message.id)) {
                return;
            }

            // Mark as processed
            this.processedMessages.add(message.id);

            // Clean up old message IDs (keep memory low)
            if (this.processedMessages.size > 1000) {
                const oldest = Array.from(this.processedMessages).slice(0, 500);
                oldest.forEach(id => this.processedMessages.delete(id));
            }

            // Broadcast to ALL local clients
            this.broadcastToLocalClients({
                type: 'message',
                payload: message,
            });
        });

        // Handle user events from Redis
        this.redis.on(CHANNELS.USER_EVENTS, (event: any) => {
            // Broadcast user join/leave to all local clients
            this.broadcastToLocalClients({
                type: event.type,
                payload: event,
            });
        });
    }

    /**
     * Broadcast a message to all clients connected to THIS server
     */
    private broadcastToLocalClients(message: WSMessage): void {
        const messageStr = JSON.stringify(message);
        let sentCount = 0;

        this.clients.forEach((client) => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(messageStr);
                sentCount++;
            }
        });

        console.log(`[WebSocket] Broadcast to ${sentCount} local clients on ${this.serverId}`);
    }

    /**
     * Heartbeat to detect dead connections
     * Pings all clients every 30 seconds
     */
    private startHeartbeat(): void {
        this.heartbeatInterval = setInterval(() => {
            this.clients.forEach((client) => {
                if (!client.isAlive) {
                    // Client didn't respond to last ping - terminate
                    console.log(`[WebSocket] Terminating inactive client: ${client.id}`);
                    client.terminate();
                    this.clients.delete(client.id);
                    return;
                }

                client.isAlive = false;
                client.ping();
            });
        }, 30000); // 30 seconds
    }

    /**
     * Get current client count for this server
     */
    getClientCount(): number {
        return this.clients.size;
    }

    /**
     * Clean up resources
     */
    async close(): Promise<void> {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
        }

        this.clients.forEach((client) => {
            client.close();
        });

        this.wss.close();
        console.log(`[WebSocket] Server closed - ${this.serverId}`);
    }
}
