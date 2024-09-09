```
  _____   ______  _____    _____  _    _  __  __  ______  _____
 |  __ \ |  ____||  __ \  / ____|| |  | ||  \/  ||  ____||  __ \
 | |__) || |__   | |  | || (___  | |  | || \  / || |__   | |__) |
 |  _  / |  __|  | |  | | \___ \ | |  | || |\/| ||  __|  |  _  /
 | | \ \ | |____ | |__| | ____) || |__| || |  | || |____ | | \ \
 |_|  \_\|______||_____/ |_____/  \____/ |_|  |_||______||_|  \_\
```

## Description

Redsumer is a GO library that provides a simple way to consume and produce messages from a Redis Stream. It is designed to be used in a microservices architecture, where a service needs to consume messages from a Redis Stream and process them. It is built on top of the [go-redis]("https://github.com/redis/go-redis") library.

## Installation

Use the package manager [go get](https://golang.org/cmd/go/#hdr-Add_dependencies_to_current_module_and_install_them) to install Redsumer.

```bash
go get github.com/enerBit/redsumer
```

## Usage

### Consuming messages from a Redis Stream

```golang
package main

import (
	"context"
    "fmt"
    "time"

    "github.com/enerBit/redsumer/v3/pkg/client"
    "github.com/enerBit/redsumer/v3/pkg/consumer"
)

func main() {
    // Redis client configuration
    redisArgs := client.RedisArgs{
		RedisHost: "localhost",
		RedisPort: 6379,
		Db:        0,
	}

	var claimBatchSize int64 = 1
    var pendingBatchSize int64 = 1
	consumerArgs := consumer.ConsumerArgs{
        // stream, group and consumer names
		StreamName:         "stream_name",
		GroupName:          "group_name",
		ConsumerName:       "consumer_name",
        // batch of messages to new messages
		BatchSize:          1,
        // batch of messages to claim, if is nil, it will dont claim messages
		ClaimBatchSize:     &claimBatchSize,
        // batch of messages to pending, if is nil, it will dont pending messages
		PendingBatchSize:   &pendingBatchSize,
        // time to block the connection
		Block:              time.Millisecond * 1,
        // MinDurationToClaim is the minimum time that a message must be in the pending state to be claimed
		MinDurationToClaim: time.Second * 1,
        // IdleStillMine is the time that a message is still mine after the last ack
		IdleStillMine:      0,
        // MaxTries is the maximum number of tries to wait for the stream to be created
		Tries:              []int{1, 2, 3, 10, 15},
    }

    ctx := context.Background()
    // Create a new consumer
	consumerClient, err := consumer.NewConsumer(ctx, redisArgs, consumerArgs)
    if err != nil {
		panic(err)
	}

    for {
        // Consume messages, get messages news, pending and claimed
		messages, err := consumerClient.Consume(ctx)
		if err != nil {
			fmt.Println(err)
		}
        // Process messages
		for _, message := range messages {
            // Check if the message is still mine
			if ok, _ := client.StillMine(ctx, message.ID); !ok {
				fmt.Println("Message", message.ID, "is not mine anymore")
				continue
			}
			fmt.Println(message.ID, message.Values)
            // Acknowledge the message
			err = consumerClient.Ack(ctx, message.ID)
            if err != nil {
			    fmt.Println(err)
            }
		}
	}
}

```

### Producing messages to a Redis Stream

```golang
package main

import (
	"context"
	"time"

    "github.com/enerBit/redsumer/v3/pkg/producer"
)

func main() {
    // Redis client configuration
    redisArgs := producer.RedisArgs{
        RedisHost: "localhost",
		RedisPort: 6379,
		Db:        0,
	}
    // Producer configuration
	producerArgs := producer.ProducerArgs{StreamName: "stream_name"}
    ctx := context.Background()
    // Create a new producer
	producer, err := producer.NewProducer(ctx, redisArgs, producerArgs)
	if err != nil {
		panic(err)
	}
    // Produce a message
    err = producer.Produce(ctx, map[string]interface{}{
        "key": "value",
    })
    if err != nil {
        panic(err)
    }
}
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
