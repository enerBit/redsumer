package redsumer

import (
	"context"

	"github.com/enerBit/redsumer/pkg/client"
	"github.com/enerBit/redsumer/pkg/consumer"
	"github.com/redis/go-redis/v9"
)

type redConsumer struct {
	args   RedConsumerArgs
	client *redis.Client
}

type RedConsumerArgs struct {
	Group        string
	Stream       string
	ConsumerName string
	RedisHost    string
	RedisPort    int
	Db           int
}

func NewRedisConsumer(args RedConsumerArgs) (redConsumer, error) {

	client, err := client.NewRedisClient(args.RedisHost, args.RedisPort, args.Db)

	if err != nil {
		return redConsumer{}, err
	}

	return redConsumer{
		args:   args,
		client: client,
	}, nil
}

func (c redConsumer) Consume(ctx context.Context) ([]redis.XMessage, error) {

	messages, err := consumer.Consume(ctx, c.client, c.args.Group, c.args.ConsumerName, c.args.Stream)
	return messages, err
}
