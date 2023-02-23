package consumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

const minIdle time.Duration = time.Millisecond * 10000

func WaitForStream(ctx context.Context, client *redis.Client, streamName string, tries []int) error {

	for _, waittime := range tries {

		time.Sleep(time.Second * time.Duration(waittime))

		streamReady, err := client.Exists(ctx, streamName).Result()
		if err != nil {
			return err
		}

		if streamReady == 1 {
			return nil
		}
	}

	return errors.New("could not connect to client")
}

func Consume(ctx context.Context, client *redis.Client, groupName string, consumerName string, streamName string) ([]redis.XMessage, error) {

	var messages []redis.XMessage

	pendingMessages, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamName,
		Group:  groupName,
		Idle:   minIdle,
		Start:  "-",
		End:    "+",
		Count:  0,
	}).Result()

	if err != nil {
		err := fmt.Errorf("error xpending: %v", err)
		return []redis.XMessage{}, err
	}

	condition := GenerateTimeAndTriesCondition(minIdle)
	result := Filter(pendingMessages, condition)

	ids := getIds(result)

	claimMessages, err := client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()

	if err != nil {
		err := fmt.Errorf("error xclaim: %v", err)
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
		Block:    time.Millisecond * 1,
		NoAck:    false,
	}).Result()

	if err != nil && !os.IsTimeout(err) {
		err := fmt.Errorf("error xreadgroup: %v", err)
		return messages, err
	}

	if len(readMessages) > 0 {
		newMessageList := readMessages[0].Messages
		messages = append(messages, newMessageList...)
	}

	return messages, nil
}
