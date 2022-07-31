package producer

import (
	"context"
	"log"

	"github.com/go-redis/redis/v9"
)

func Produce(client *redis.Client, streamName string, message map[string]interface{}) {

	ctx := context.Background()

	err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		MaxLen: 0,
		Values: message,
	}).Err()

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("message sent to stream '%s'", streamName)
}
