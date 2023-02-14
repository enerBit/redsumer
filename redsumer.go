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

// given a list of tries([]int) this method will wait until the stream is ready
// if the stream is not ready after the tries it will return an erro
// the numbers in the tries array represent the number of seconds that the loop will wait until it tries again
func (c redConsumer) WaitForStream(ctx context.Context, tries []int) error {

	err := consumer.WaitForStream(ctx, c.client, c.args.Stream, tries)
	return err
}

// Creates a xonumer group for the redis stream
// if the group already exist it will return an error
func (c redConsumer) CreateGroup(ctx context.Context, start string) error {
	err := c.client.XGroupCreate(ctx, c.args.Stream, c.args.Group, start).Err()
	return err
}

// Get the raw redis client from the librari redis-go
func (c redConsumer) RawRedis() *redis.Client {
	return c.client
}

func (c redConsumer) GetArgs() RedConsumerArgs {
	return c.args
}
