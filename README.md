# Node.js + Kafka TypeScript

Kafka Producer/Consumer with TypeScript and Docker.

## Quick Start

```bash
# Start Kafka
docker compose -f docker/docker-compose.yml up -d

# Watch consumer logs
docker logs -f kafka-consumer

# Run interactive producer (development only)
npm run dev:producer

# Send single message (production)
npm run producer "Your message here"
```

## Interactive Producer

**Interactive mode only works in development (NODE_ENV=development)**

```bash
npm install
npm run dev:producer
```

- **Type message + Enter** → Send custom message
- **Enter only** → Send "Hello Kafka!" with timestamp  
- **Ctrl+C** → Exit

**Production mode (single message only):**
```bash
npm run producer "Your message here"
```

## Scripts

```bash
npm run producer "msg"  # Send single message (production)
npm run consumer        # Start consumer
npm run dev:producer    # Interactive producer (development)
npm run dev:consumer    # Consumer with hot reload
npm run build          # Compile TypeScript
npm run clean          # Clean build directory
npm run docker:up      # Start Docker services
npm run docker:down    # Stop Docker services
npm run docker:logs    # View all service logs
```

## Docker Commands

```bash
docker compose -f docker/docker-compose.yml up -d    # Start all
docker logs -f kafka-consumer                        # View logs
docker exec -it kafka-producer sh                    # Interactive producer in Docker
docker compose -f docker/docker-compose.yml down     # Stop all
```

## Web UI

**Kafka UI**: http://localhost:8080

## Troubleshooting

### Consumer Connection Errors (ECONNREFUSED)

If you see connection errors like `ECONNREFUSED 172.25.0.3:29092`, this usually means Kafka failed to start properly:

```bash
# Clean restart (recommended solution)
docker compose -f docker/docker-compose.yml down
docker compose -f docker/docker-compose.yml up -d

# Check Kafka status
docker logs kafka
```

**Common causes:**
- Stale ZooKeeper sessions from improper shutdown
- Port conflicts
- Insufficient system resources

### Verify Setup

```bash
# Check all services are running
docker compose -f docker/docker-compose.yml ps

# Test message flow
npm run producer "Test message"
docker logs --tail 5 kafka-consumer
```