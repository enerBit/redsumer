package producer

import (
	"context"

	"github.com/enerBit/redsumer/v2/pkg/client"
	"github.com/redis/go-redis/v9"
)

type Producer struct {
	Client       *redis.Client
	RedisArgs    client.RedisArgs
	ProducerArgs ProducerArgs
}

type ProducerArgs struct {
	StreamName string
}
// NewProducer creates a new instance of the Producer struct.
// It takes a context.Context, RedisArgs, and ProducerArgs as input parameters.
// It returns a pointer to the created Producer and an error, if any.
func NewProducer(ctx context.Context, redisArgs client.RedisArgs, producerArgs ProducerArgs) (*Producer, error) {
	redisClient, err := redisArgs.NewRedisClient(ctx)
	if err != nil {
		return nil, err
	}

	producer := &Producer{
		Client:       redisClient,
		RedisArgs:    redisArgs,
		ProducerArgs: producerArgs,
	}

	return producer, nil
}

// Produce sends a message to the specified stream.
// It takes a context and a message map as input.
// Returns an error if there was a problem sending the message.
func (p *Producer) Produce(ctx context.Context, message map[string]interface{}) error {
	err := p.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.ProducerArgs.StreamName,
		MaxLen: 0,
		Values: message,
	}).Err()
	if err != nil {
		return err
	}

	return nil
}
