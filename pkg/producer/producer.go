package producer

import (
	"context"

	"github.com/redis/go-redis/v9"
)

func Produce(client *redis.Client, streamName string, message map[string]interface{}) error {

	ctx := context.Background()

	err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		MaxLen: 0,
		Values: message,
	}).Err()

	if err != nil {
		return err
	}

	return nil
}
