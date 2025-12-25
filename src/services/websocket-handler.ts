/**
 * ============================================
 * SOCKET.IO HANDLER
 * ============================================
 * 
 * This module manages Socket.io connections for a single server instance.
 * 
 * KEY CONCEPTS:
 * 
 * 1. LOCAL CONNECTIONS: Each server tracks only its own connected clients
 * 2. MESSAGE DEDUPLICATION: We track processed message IDs to avoid duplicates (from Redis)
 * 3. REDIS INTEGRATION: Messages are published to Redis for cross-server delivery
 * 
 * MESSAGE FLOW: Same as before, but using Socket.io's event system
 */

import { Server as SocketServer, Socket } from 'socket.io';
import { v4 as uuidv4 } from 'uuid';
import { RedisPubSub, ChatMessage, CHANNELS } from './redis-pubsub';
import { Server } from 'http';

// Extended Socket type with user info
interface ChatSocket extends Socket {
    userId?: string;
    username?: string;
}

export class WebSocketHandler {
    private io: SocketServer;
    private redis: RedisPubSub;
    private serverId: string;
    private processedMessages: Set<string> = new Set(); // For deduplication

    constructor(server: Server, redis: RedisPubSub) {
        this.redis = redis;
        this.serverId = redis.getServerId();

        // Create Socket.io server attached to HTTP server
        this.io = new SocketServer(server, {
            cors: {
                origin: "*",
                methods: ["GET", "POST"]
            }
        });

        this.setupSocketHandlers();
        this.setupRedisHandlers();

        console.log(`[Socket.io] Server initialized - ${this.serverId}`);
    }

    /**
     * Set up Socket.io connection handlers
     */
    private setupSocketHandlers(): void {
        this.io.on('connection', (socket: ChatSocket) => {
            console.log(`[Socket.io] New connection: ${socket.id} on ${this.serverId}`);

            // Handle user joining the chat
            socket.on('join', async (payload: { username: string }) => {
                await this.handleJoin(socket, payload);
            });

            // Handle incoming chat messages
            socket.on('message', async (payload: { content: string }) => {
                await this.handleChatMessage(socket, payload);
            });

            // Handle client disconnect
            socket.on('disconnect', () => {
                this.handleClientDisconnect(socket);
            });

            // Handle errors
            socket.on('error', (error) => {
                console.error(`[Socket.io] Client error ${socket.id}:`, error.message);
            });
        });
    }

    /**
     * Handle user joining the chat
     */
    private async handleJoin(socket: ChatSocket, payload: { username: string }): Promise<void> {
        socket.userId = uuidv4();
        socket.username = payload.username || `User-${socket.id.slice(0, 4)}`;

        console.log(`[Socket.io] User joined: ${socket.username} (${socket.userId}) on ${this.serverId}`);

        // Send recent message history
        const history = await this.redis.getRecentMessages(50);
        socket.emit('history', {
            messages: history,
            serverId: this.serverId,
        });

        // Notify all servers about new user
        await this.redis.publishUserEvent({
            type: 'join',
            userId: socket.userId,
            username: socket.username,
            serverId: this.serverId,
            timestamp: Date.now(),
        });

        // Send confirmation to the joining client
        socket.emit('join_success', {
            userId: socket.userId,
            username: socket.username,
            serverId: this.serverId,
            message: `Welcome ${socket.username}! You are connected to ${this.serverId}`,
        });
    }

    /**
     * Handle incoming chat message from a client
     */
    private async handleChatMessage(socket: ChatSocket, payload: { content: string }): Promise<void> {
        if (!socket.userId || !socket.username) {
            socket.emit('error_msg', { message: 'Please join first' });
            return;
        }

        const chatMessage: ChatMessage = {
            id: uuidv4(),
            userId: socket.userId,
            username: socket.username,
            content: payload.content,
            timestamp: Date.now(),
            serverId: this.serverId,
        };

        // Store and Publish to Redis
        await this.redis.storeMessage(chatMessage);
        await this.redis.publishMessage(chatMessage);

        console.log(`[Socket.io] Message from ${socket.username}: "${payload.content.slice(0, 50)}..."`);
    }

    /**
     * Handle client disconnection
     */
    private async handleClientDisconnect(socket: ChatSocket): Promise<void> {
        if (socket.userId && socket.username) {
            await this.redis.publishUserEvent({
                type: 'leave',
                userId: socket.userId,
                username: socket.username,
                serverId: this.serverId,
                timestamp: Date.now(),
            });

            console.log(`[Socket.io] User left: ${socket.username} (${socket.userId}) from ${this.serverId}`);
        }
    }

    /**
     * Set up handlers for Redis Pub/Sub messages
     */
    private setupRedisHandlers(): void {
        this.redis.on(CHANNELS.CHAT_MESSAGES, (message: ChatMessage) => {
            if (this.processedMessages.has(message.id)) return;
            this.processedMessages.add(message.id);

            // Deduplication cleanup
            if (this.processedMessages.size > 1000) {
                const arr = Array.from(this.processedMessages);
                this.processedMessages = new Set(arr.slice(500));
            }

            // Broadcast to all local clients using Socket.io
            this.io.emit('message', message);
        });

        this.redis.on(CHANNELS.USER_EVENTS, (event: any) => {
            this.io.emit(event.type, event);
        });
    }

    /**
     * Get current client count for this server
     */
    getClientCount(): number {
        return this.io.engine.clientsCount;
    }

    /**
     * Clean up resources
     */
    async close(): Promise<void> {
        this.io.close();
        console.log(`[Socket.io] Server closed - ${this.serverId}`);
    }
}
