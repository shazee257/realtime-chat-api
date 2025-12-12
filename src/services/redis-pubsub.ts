/**
 * ============================================
 * REDIS PUB/SUB SERVICE
 * ============================================
 * 
 * This service handles cross-server communication using Redis Pub/Sub.
 * 
 * HOW IT WORKS:
 * 1. Each server subscribes to a Redis channel (e.g., 'chat:messages')
 * 2. When a message arrives on any server, it publishes to Redis
 * 3. Redis broadcasts the message to ALL subscribed servers
 * 4. Each server then broadcasts to its connected WebSocket clients
 * 
 * ARCHITECTURE:
 * 
 *   Client A ──► Server 1 ──┐
 *                           │
 *   Client B ──► Server 1 ──┼──► Redis Pub/Sub ──► All Servers ──► All Clients
 *                           │
 *   Client C ──► Server 2 ──┘
 *   Client D ──► Server 2
 *   Client E ──► Server 3
 * 
 * We use TWO Redis connections:
 * - Publisher: For publishing messages
 * - Subscriber: For receiving messages (Redis requires separate connection for subscriptions)
 */

import Redis from 'ioredis';
import { EventEmitter } from 'events';

// Message structure for chat messages
export interface ChatMessage {
    id: string;           // Unique message ID for deduplication
    userId: string;       // Who sent the message
    username: string;     // Display name
    content: string;      // Message content
    timestamp: number;    // Unix timestamp
    serverId: string;     // Which server received this message originally
}

// Channel names for Redis Pub/Sub
export const CHANNELS = {
    CHAT_MESSAGES: 'chat:messages',
    USER_EVENTS: 'chat:users',
} as const;

export class RedisPubSub extends EventEmitter {
    private publisher: Redis;
    private subscriber: Redis;
    private serverId: string;
    private isConnected: boolean = false;

    constructor(serverId: string) {
        super();
        this.serverId = serverId;

        // Create Redis configuration
        const redisConfig = {
            host: process.env.REDIS_HOST || 'localhost',
            port: parseInt(process.env.REDIS_PORT || '6379'),
            password: process.env.REDIS_PASSWORD || undefined,
            retryStrategy: (times: number) => {
                // Retry connection with exponential backoff
                const delay = Math.min(times * 100, 3000);
                console.log(`[Redis] Reconnecting in ${delay}ms...`);
                return delay;
            },
        };

        // Create TWO separate connections
        // Publisher: Used for sending messages
        this.publisher = new Redis(redisConfig);

        // Subscriber: Used for receiving messages
        // (Redis requires dedicated connection for subscriptions)
        this.subscriber = new Redis(redisConfig);

        this.setupEventHandlers();
    }

    /**
     * Set up Redis connection and message handlers
     */
    private setupEventHandlers(): void {
        // Publisher connection events
        this.publisher.on('connect', () => {
            console.log(`[Redis Publisher] Connected - Server: ${this.serverId}`);
        });

        this.publisher.on('error', (error) => {
            console.error(`[Redis Publisher] Error:`, error.message);
        });

        // Subscriber connection events
        this.subscriber.on('connect', () => {
            console.log(`[Redis Subscriber] Connected - Server: ${this.serverId}`);
            this.isConnected = true;
        });

        this.subscriber.on('error', (error) => {
            console.error(`[Redis Subscriber] Error:`, error.message);
        });

        // Handle incoming messages from Redis
        this.subscriber.on('message', (channel: string, message: string) => {
            try {
                const data = JSON.parse(message);

                // Emit the message so WebSocket handler can broadcast it
                // The channel name tells us what type of message it is
                this.emit(channel, data);

                console.log(`[Redis] Received on ${channel}:`, {
                    messageId: data.id,
                    from: data.username,
                    originServer: data.serverId,
                    currentServer: this.serverId,
                });
            } catch (error) {
                console.error('[Redis] Failed to parse message:', error);
            }
        });
    }

    /**
     * Subscribe to Redis channels
     * Call this after creating the instance
     */
    async subscribe(): Promise<void> {
        try {
            // Subscribe to all channels we care about
            await this.subscriber.subscribe(
                CHANNELS.CHAT_MESSAGES,
                CHANNELS.USER_EVENTS
            );

            console.log(`[Redis] Subscribed to channels:`, Object.values(CHANNELS));
        } catch (error) {
            console.error('[Redis] Failed to subscribe:', error);
            throw error;
        }
    }

    /**
     * Publish a chat message to all servers
     * 
     * FLOW:
     * 1. User sends message via WebSocket to Server A
     * 2. Server A calls this method
     * 3. Message is published to Redis
     * 4. ALL servers (including A) receive the message
     * 5. Each server broadcasts to its WebSocket clients
     */
    async publishMessage(message: ChatMessage): Promise<void> {
        try {
            const messageStr = JSON.stringify(message);

            // Publish to Redis - all subscribed servers will receive this
            await this.publisher.publish(CHANNELS.CHAT_MESSAGES, messageStr);

            console.log(`[Redis] Published message:`, {
                messageId: message.id,
                to: CHANNELS.CHAT_MESSAGES,
                server: this.serverId,
            });
        } catch (error) {
            console.error('[Redis] Failed to publish message:', error);
            throw error;
        }
    }

    /**
     * Publish user events (join/leave)
     */
    async publishUserEvent(event: {
        type: 'join' | 'leave';
        userId: string;
        username: string;
        serverId: string;
        timestamp: number;
    }): Promise<void> {
        try {
            await this.publisher.publish(CHANNELS.USER_EVENTS, JSON.stringify(event));
        } catch (error) {
            console.error('[Redis] Failed to publish user event:', error);
        }
    }

    /**
     * Store message in Redis for guaranteed delivery
     * New users can retrieve recent messages they might have missed
     */
    async storeMessage(message: ChatMessage): Promise<void> {
        try {
            const key = 'chat:history';
            const messageStr = JSON.stringify(message);

            // Add to sorted set with timestamp as score (for ordering)
            await this.publisher.zadd(key, message.timestamp, messageStr);

            // Keep only last 100 messages (memory management)
            await this.publisher.zremrangebyrank(key, 0, -101);
        } catch (error) {
            console.error('[Redis] Failed to store message:', error);
        }
    }

    /**
     * Get recent messages for new connections
     * Ensures no missed messages when users join
     */
    async getRecentMessages(count: number = 50): Promise<ChatMessage[]> {
        try {
            const key = 'chat:history';

            // Get last N messages (sorted by timestamp)
            const messages = await this.publisher.zrange(key, -count, -1);

            return messages.map((msg) => JSON.parse(msg));
        } catch (error) {
            console.error('[Redis] Failed to get recent messages:', error);
            return [];
        }
    }

    /**
     * Clean up connections
     */
    async disconnect(): Promise<void> {
        await this.subscriber.unsubscribe();
        await this.subscriber.quit();
        await this.publisher.quit();
        this.isConnected = false;
        console.log(`[Redis] Disconnected - Server: ${this.serverId}`);
    }

    getServerId(): string {
        return this.serverId;
    }

    isReady(): boolean {
        return this.isConnected;
    }
}
