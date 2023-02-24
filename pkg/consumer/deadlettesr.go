package consumer

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func AcknowledgeDeadLetters(ctx context.Context, client *redis.Client, streamName, deadStreamName, groupName, consumerName string, threshold time.Duration) error {

	pendingMessage, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamName,
		Group:  groupName,
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

	claimedMessages, err := client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()

	if err != nil {
		return err
	}

	for _, val := range claimedMessages {

		_, err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: deadStreamName,
			MaxLen: 0,
			Values: val.Values,
		}).Result()

		if err != nil {
			return err
		}

		_, err = client.XAck(ctx, streamName, groupName, val.ID).Result()
		if err != nil {
			return err
		}
	}

	return nil
}
