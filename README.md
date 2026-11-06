
# Kafka Lab

Spring Boot application demonstrating Apache Kafka Producer, Consumer, retry mechanism and Dead Letter Topic (DLT).

## Tech Stack

- Java 21 / Spring Boot 4.1.2
- Spring Kafka
- Apache Kafka 4.4.0
- Docker Compose / Maven

## Features

- REST API for sending messages
- Kafka Producer and Consumer
- Retry: 2 attempts with a 1-second delay
- Dead Letter Topic (`messages-dlt`)
- Automatic topic creation

## Getting Started

**Requirements:** Java 21, Docker Desktop.

1. Start Kafka:

```bash
docker compose up -d
```

2. Run Spring Boot (Windows):

```powershell
.\mvnw.cmd spring-boot:run
```

3. Send a message:

```powershell
curl.exe -X POST http://localhost:8080/messages -H "Content-Type: text/plain" --data-raw "Hello Kafka!"
```

Check the application logs for successful consumption.

## Testing DLQ

Send a message that triggers an exception:

```powershell
curl.exe -X POST http://localhost:8080/messages -H "Content-Type: text/plain" --data-raw "FAIL"
```

After the initial attempt and 2 retries, the message is published to `messages-dlt`.

Check the application logs for:

```
Сообщение попало в DLQ: FAIL
```

## Stop Kafka

```bash
docker compose down
```
