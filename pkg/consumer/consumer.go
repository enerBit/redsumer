package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
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

	claimMessages, _, err := client.XAutoClaim(context.Background(), &redis.XAutoClaimArgs{
		Group:    groupName,
		Consumer: consumerName,
		Stream:   streamName,
		MinIdle:  time.Second,
		Count:    500,
		Start:    "0-0",
	}).Result()
	if err != nil {
		log.Println("cannot get messages")
		return claimMessages, err
	}

	if len(claimMessages) > 0 {
		log.Println("claimMessages", len(claimMessages))
	}

	entries, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    500,
		NoAck:    false,
		Block:    1,
	}).Result()
	if err != nil && !os.IsTimeout(err) {
		err := fmt.Errorf("error xreadgroup: %v", err)
		return entries[0].Messages, err
	}
	if len(entries) > 0 {
		log.Println("new entries", len(entries))
		newMessageList := entries[0].Messages
		claimMessages = append(claimMessages, newMessageList...)
	}
	return claimMessages, nil
}

func ConsumePendingOneByOne(ctx context.Context, client *redis.Client, groupName string, consumerName string, streamName string, id string) (*redis.XMessage, error) {

	entries, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, id},
		Count:    1,
		NoAck:    false,
		Block:    time.Duration(1) * time.Millisecond,
	}).Result()

	if err != nil && !os.IsTimeout(err) {
		err := fmt.Errorf("error xreadgroup: %v", err)
		return nil, err
	}

	if len(entries) > 0 && len(entries[0].Messages) > 0 {
		return &entries[0].Messages[0], nil
	}

	return nil, nil
}
