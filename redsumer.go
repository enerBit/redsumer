package redsumer

import (
	"context"
	"fmt"
	"time"

	"github.com/enerBit/redsumer/pkg/client"
	"github.com/enerBit/redsumer/pkg/consumer"
	"github.com/enerBit/redsumer/pkg/producer"
	"github.com/redis/go-redis/v9"
)

type RedConsumer struct {
	args   RedConsumerArgs
	client *redis.Client
}

type RedProducer struct {
	args   RedProducerArgs
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

type RedProducerArgs struct {
	RedisHost string
	RedisPort int
	Db        int
}

func NewRedisConsumer(args RedConsumerArgs) (RedConsumer, error) {

	client, err := client.NewRedisClient(args.RedisHost, args.RedisPort, args.Db)

	if err != nil {
		return RedConsumer{}, err
	}

	return RedConsumer{
		args:   args,
		client: client,
	}, nil
}

func (c RedConsumer) Consume(ctx context.Context) ([]redis.XMessage, error) {

	messages, err := consumer.Consume(ctx, c.client, c.args.Group, c.args.ConsumerName, c.args.Stream)
	return messages, err
}

// given a list of tries([]int) this method will wait until the stream is ready
// if the stream is not ready after the tries it will return an erro
// the numbers in the tries array represent the number of seconds that the loop will wait until it tries again
func (c RedConsumer) WaitForStream(ctx context.Context, tries []int) error {

	err := consumer.WaitForStream(ctx, c.client, c.args.Stream, tries)
	return err
}

func (c RedConsumer) Acknowledge(ctx context.Context, ids ...string) error {
	err := c.client.XAck(ctx, c.args.Stream, c.args.Group, ids...).Err()
	return err
}

func (c RedConsumer) AcknowledgeDeadLetters(ctx context.Context, threshold time.Duration) error {

	deadStreamName := fmt.Sprintf("dead:letters:%s", c.client)
	err := consumer.AcknowledgeDeadLetters(ctx, c.client, c.args.Stream, deadStreamName, c.args.Group, c.args.ConsumerName, threshold)

	return err
}

// Creates a xonumer group for the redis stream
// if the group already exist it will return an error
func (c RedConsumer) CreateGroup(ctx context.Context, start string) error {
	err := c.client.XGroupCreate(ctx, c.args.Stream, c.args.Group, start).Err()
	return err
}

// Get the raw redis client from the librari redis-go
func (c RedConsumer) RawRedis() *redis.Client {
	return c.client
}

func (c RedConsumer) GetArgs() RedConsumerArgs {
	return c.args
}

func NewRedisProducer(args RedProducerArgs) (RedProducer, error) {

	client, err := client.NewRedisClient(args.RedisHost, args.RedisPort, args.Db)

	if err != nil {
		return RedProducer{}, err
	}

	return RedProducer{
		args:   args,
		client: client,
	}, nil
}

func (p RedProducer) Produce(streamName string, message map[string]interface{}) error {

	err := producer.Produce(p.client, streamName, message)
	return err
}
