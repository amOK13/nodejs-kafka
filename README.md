# Node.js + Kafka TypeScript

Kafka Producer/Consumer with TypeScript and Docker.

## Quick Start

```bash
# Start Kafka
docker compose -f docker/docker-compose.yml up -d

# Watch consumer logs
docker logs -f kafka-consumer

# Run interactive producer
npm run dev:producer
```

## Interactive Producer

```bash
npm install
npm run dev:producer
```

- **Type message + Enter** → Send custom message
- **Enter only** → Send "Hello Kafka!" with timestamp
- **Ctrl+C** → Exit

Send direct message:
```bash
npm run dev:producer "Your message here"
```

## Scripts

```bash
npm run dev:producer    # Interactive producer
npm run dev:consumer    # Consumer listener
npm run build          # Compile TypeScript
```

## Docker Commands

```bash
docker compose -f docker/docker-compose.yml up -d    # Start all
docker logs -f kafka-consumer                        # View logs
docker compose -f docker/docker-compose.yml down     # Stop all
```

## Web UI

**Kafka UI**: http://localhost:8080