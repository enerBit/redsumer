package consumer

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/enerBit/redsumer/pkg/client"
	"github.com/redis/go-redis/v9"
)

type ConsumerArgs struct {
	StreamName         string
	GroupName          string
	ConsumerName       string
	BatchSize          int64
	ClaimBatchSize     *int64
	PendingBatchSize   *int64
	Block              time.Duration
	MinDurationToClaim time.Duration
	IdleStillMine      time.Duration
	Tries              []int
}

type Consumer struct {
	ConsumerArgs           ConsumerArgs
	RedisArgs              client.RedisArgs
	client                 *redis.Client
	LatestPendingMessageId string
}

const (
	NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR = ">"
	FIRST_ID_INSIDE_THE_STREAM                = "0-0"
)
const (
	BUSYGROUP = "BUSYGROUP Consumer Group name already exists"
	NOGROUP   = "NOGROUP No such key"
)

// NewConsumer creates a new Consumer instance.
// It initializes a Redis client using the provided RedisArgs and sets up the consumer with the given ConsumerArgs.
// It waits for the stream to be available and creates a consumer group.
// If any error occurs during the process, it returns nil and the error.
// Otherwise, it returns the created Consumer instance and nil error.
func NewConsumer(ctx context.Context, redisArgs client.RedisArgs, consumerArgs ConsumerArgs) (*Consumer, error) {
	log.Println("Creating new consumer")
	redisClient, err := redisArgs.NewRedisClient(ctx)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		ConsumerArgs:           consumerArgs,
		client:                 redisClient,
		LatestPendingMessageId: FIRST_ID_INSIDE_THE_STREAM,
	}

	err = consumer.createGroup(ctx)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

// createGroup creates a consumer group for processing messages from a stream.
// It waits for the stream to be available and then creates the group using the provided arguments.
// If the group already exists, it returns without an error.
// If any error occurs during the process, it is returned.
func (c *Consumer) createGroup(ctx context.Context) error {
	log.Println("Creating consumer group", c.ConsumerArgs.GroupName)
	err := c.waitForStream(ctx)
	if err != nil {
		return err
	}
	_, err = c.client.XGroupCreate(ctx, c.ConsumerArgs.StreamName, c.ConsumerArgs.GroupName, FIRST_ID_INSIDE_THE_STREAM).Result()
	if err != nil {
		if err.Error() != BUSYGROUP {
			return err
		}
	}
	return nil
}

// waitForStream waits for the stream to be ready by checking its existence in the Redis client.
// It retries for the specified number of times with a delay between each attempt.
// If the stream is ready, it returns nil. Otherwise, it returns an error.
func (c *Consumer) waitForStream(ctx context.Context) error {
	log.Println("Waiting for stream to be ready")
	for _, waittime := range c.ConsumerArgs.Tries {
		time.Sleep(time.Second * time.Duration(waittime))
		streamReady, err := c.client.Exists(ctx, c.ConsumerArgs.StreamName).Result()
		if err != nil {
			return err
		}
		if streamReady == 1 {
			return nil
		}
	}

	return errors.New("could not connect to client")
}

// StillMine checks if a message with the given messageId is still pending for consumption.
// It returns true if the message is still pending, otherwise false.
// If there is an error while checking, it returns the error.
func (c *Consumer) StillMine(ctx context.Context, messageId string) (bool, error) {
	log.Println("Checking if message", messageId, "is still pending")
	pending, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   c.ConsumerArgs.StreamName,
		Group:    c.ConsumerArgs.GroupName,
		Consumer: c.ConsumerArgs.ConsumerName,
		Idle:     c.ConsumerArgs.IdleStillMine,
		Start:    messageId,
		End:      messageId,
		Count:    1,
	}).Result()
	if err != nil {
		return false, err
	}

	return len(pending) > 0, nil
}

// Ack acknowledges a message with the given message ID in the consumer group.
// It returns an error if there was a problem acknowledging the message.
func (c *Consumer) Ack(ctx context.Context, messageId string) error {
	log.Println("Acknowledging message", messageId)
	_, err := c.client.XAck(ctx, c.ConsumerArgs.StreamName, c.ConsumerArgs.GroupName, messageId).Result()
	if err != nil {
		return err
	}
	return nil
}

// newMessages retrieves new messages from a Redis stream for the consumer.
// It uses the XReadGroup command to read messages from the specified stream,
// using the consumer group and name provided in the Consumer struct.
// The function returns a slice of redis.XMessage, which contains the retrieved messages,
// and an error if any occurred during the retrieval process.
func (c *Consumer) newMessages(ctx context.Context) ([]redis.XMessage, error) {
	log.Println("Retrieving new messages")
	resp, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.ConsumerArgs.GroupName,
		Consumer: c.ConsumerArgs.ConsumerName,
		Streams:  []string{c.ConsumerArgs.StreamName, NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR},
		Count:    c.ConsumerArgs.BatchSize,
		Block:    c.ConsumerArgs.Block,
		NoAck:    false,
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	var message []redis.XMessage
	for _, v := range resp {
		if v.Stream == c.ConsumerArgs.StreamName {
			message = v.Messages
			break
		}
	}
	return message, nil
}

// pendingMessages retrieves pending messages from a Redis stream.
// It returns a slice of redis.XMessage representing the pending messages and an error if any.
func (c *Consumer) pendingMessages(ctx context.Context) ([]redis.XMessage, error) {
	log.Println("Retrieving pending messages")
	resp, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.ConsumerArgs.GroupName,
		Consumer: c.ConsumerArgs.ConsumerName,
		Streams:  []string{c.ConsumerArgs.StreamName, c.LatestPendingMessageId},
		Count:    *c.ConsumerArgs.PendingBatchSize,
		Block:    c.ConsumerArgs.Block,
		NoAck:    false,
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	var message []redis.XMessage
	for _, v := range resp {
		if v.Stream == c.ConsumerArgs.StreamName {
			message = v.Messages
			if len(message) != 0 {
				c.LatestPendingMessageId = message[len(message)-1].ID
			} else {
				c.LatestPendingMessageId = FIRST_ID_INSIDE_THE_STREAM
			}
			break
		}
	}
	return message, nil
}

// claimedMessages returns a slice of claimed messages from the Redis stream.
// It uses the XAutoClaim method of the Redis client to automatically claim messages
// from the specified stream and group. The minimum idle duration to claim a message
// is determined by the MinDurationToClaim field of the ConsumerArgs struct.
// The Start field specifies the ID of the first message to claim, and the Count field
// determines the number of messages to claim in a batch. The Consumer field specifies
// the name of the consumer. If an error occurs during the claiming process, it is returned.
// If the error is not equal to the REDIS_NIL error, it is returned as is.
// Otherwise, the claimed messages are returned along with a nil error.
func (c *Consumer) claimedMessages(ctx context.Context) ([]redis.XMessage, error) {
	log.Println("Retrieving claimed messages")
	messages, _, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   c.ConsumerArgs.StreamName,
		Group:    c.ConsumerArgs.GroupName,
		Consumer: c.ConsumerArgs.ConsumerName,
		MinIdle:  c.ConsumerArgs.MinDurationToClaim,
		Start:    FIRST_ID_INSIDE_THE_STREAM,
		Count:    *c.ConsumerArgs.ClaimBatchSize,
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	return messages, nil
}

// validateError checks if the given error contains a specific error message and performs an action accordingly.
// If the error message contains NOGROUP, it calls the createGroup method to create a group.
// Otherwise, it returns the original error.
func (c *Consumer) validateError(ctx context.Context, err error) error {
	log.Println("Validating error", err)
	if strings.Contains(err.Error(), NOGROUP) {
		return c.createGroup(ctx)
	}
	return err
}

// Consume consumes messages from Redis.
// It first tries to fetch new messages, then pending messages, and finally claimed messages.
// If any messages are found, they are returned along with a nil error.
// If no messages are found, it returns nil and nil error.
func (c *Consumer) Consume(ctx context.Context) ([]redis.XMessage, error) {
	log.Println("Processing new messages")
	messages, err := c.newMessages(ctx)
	if err != nil {
		return nil, c.validateError(ctx, err)
	}
	if len(messages) > 0 {
		return messages, nil
	}
	log.Println("Processing pending messages")
	if c.ConsumerArgs.PendingBatchSize != nil {
		messages, err = c.pendingMessages(ctx)
		if err != nil {
			return nil, c.validateError(ctx, err)
		}
		if len(messages) > 0 {
			return messages, nil
		}
	}
	log.Println("Processing claimed messages")
	if c.ConsumerArgs.ClaimBatchSize != nil {
		messages, err = c.claimedMessages(ctx)
		if err != nil {
			return nil, c.validateError(ctx, err)
		}
		if len(messages) > 0 {
			return messages, nil
		}
	}
	return nil, nil
}
