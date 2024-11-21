# kafkabuff
kafkabuff is a thread-safe ring buffer implementation designed for efficient handling of Kafka messages

## Features

- Thread-safe operations for adding and retrieving messages.
- Batch retrieval for efficient Kafka message processing.
- Automatic overwriting of the oldest messages when the buffer is full.
- Easy integration with Sarama Kafka client.

## Installation

To use `kafkabuff`, simply run:

```bash
go get github.com/vagbundor/kafkabuff
