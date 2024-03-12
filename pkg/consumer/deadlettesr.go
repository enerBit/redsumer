package consumer

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const minIdle time.Duration = time.Millisecond * 10000

func (c *Consumer) AcknowledgeDeadLetters(ctx context.Context, deadStreamName string, threshold time.Duration) error {
	pendingMessage, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: c.ConsumerArgs.StreamName,
		Group:  c.ConsumerArgs.GroupName,
		Idle:   minIdle,
		Start:  "-",
		End:    "+",
		Count:  0,
	}).Result()
	if err != nil {
		return err
	}

	condition := ToleranceCondition(threshold)
	deadLetters := Filter(pendingMessage, condition)
	ids := getIds(deadLetters)

	claimedMessages, err := c.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   c.ConsumerArgs.StreamName,
		Group:    c.ConsumerArgs.GroupName,
		Consumer: c.ConsumerArgs.ConsumerName,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()
	if err != nil {
		return err
	}

	for _, val := range claimedMessages {
		_, err := c.client.XAdd(ctx, &redis.XAddArgs{
			Stream: deadStreamName,
			MaxLen: 0,
			Values: val.Values,
		}).Result()
		if err != nil {
			return err
		}

		err = c.Ack(ctx, val.ID)
		if err != nil {
			return err
		}
	}

	return nil
}
