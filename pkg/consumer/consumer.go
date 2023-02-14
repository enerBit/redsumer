package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

const minIdle time.Duration = time.Millisecond * 10000

func WaitForStream(ctx context.Context, client *redis.Client, streamName string, tries []int) error {

	for _, waittime := range tries {

		streamReady, err := client.Exists(ctx, streamName).Result()
		if err != nil {
			return err
		}

		if streamReady == 1 {
			return nil
		}

		time.Sleep(time.Second * time.Duration(waittime))
	}

	return errors.New("could not connect to client")
}

func Consume(ctx context.Context, client *redis.Client, groupName string, consumerName string, streamName string) ([]redis.XMessage, error) {

	var messages []redis.XMessage

	claimMessages, _, err := client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minIdle,
		Count:    1000,
		Start:    "0-0",
	}).Result()

	if err != nil {
		return []redis.XMessage{}, err
	}

	if len(claimMessages) > 0 {
		messages = append(messages, claimMessages...)
	}

	readMessages, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    0,
		Block:    0,
		NoAck:    false,
	}).Result()

	if err != nil {
		return []redis.XMessage{}, err
	}

	if len(readMessages) > 0 {
		newMessageList := readMessages[0].Messages
		messages = append(messages, newMessageList...)
	}

	return messages, nil
}
