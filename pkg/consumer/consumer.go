package consumer

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/go-redis/redis/v9"
)

var maxWaitSecondsForStream float64 = 20
var minIdle time.Duration = time.Millisecond * 10000

func getWaitTime(n float64) float64 {
	max := 1000
	min := -1000

	return math.Pow(2, n) + float64(rand.Intn(max-min)+min)/1000
}

func waitForStream(client *redis.Client, streamName string) {

	ctx := context.Background()

	n := 0.0
	var streamReady int64

	for streamReady == 0 {
		streamReady, _ = client.Exists(ctx, streamName).Result()
		if streamReady == 0 {
			waitTime := getWaitTime(n)

			if waitTime > maxWaitSecondsForStream {
				notFoundStreamMessage := fmt.Sprintf("Stream '%s' was not found", streamName)
				log.Panic(notFoundStreamMessage)
			}

			log.Printf("Stream '%s' not ready yet", streamName)
			log.Printf("Waiting for %.2f seconds more...", waitTime)

			time.Sleep(time.Second * time.Duration(waitTime))
			n++
		}
	}

	log.Printf("Stream status: %c", streamReady)
	log.Printf("Stream '%s' ready", streamName)
}

func checkConsumerGroupExists(client *redis.Client, streamName, groupName, start string) {
	ctx := context.Background()
	err := client.XGroupCreate(ctx, streamName, groupName, start).Err()

	log.Printf("Creating consumer group %s", groupName)
	if err != nil {
		log.Printf("Could not create stream comsumer group %s", groupName)
		log.Printf("Consumer group already exists")
	}
}

func AcknowledgeMessage(client *redis.Client, groupName, streamName, Id string) {
	ctx := context.Background()
	client.XAck(ctx, streamName, groupName, Id)
}

func Consume(client *redis.Client, groupName string, consumerName string, streamName string) []redis.XMessage {

	var messages []redis.XMessage

	ctx := context.Background()

	waitForStream(client, streamName)
	checkConsumerGroupExists(client, streamName, groupName, "0")

	log.Print("Claiming pending messages")

	ClaimMessages, _, _ := client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minIdle,
		Count:    1500,
		Start:    "0-0",
	}).Result()

	if len(ClaimMessages) > 0 {
		messages = append(messages, ClaimMessages...)
		log.Printf("Found %d queued messages", len(ClaimMessages))
	}

	log.Println("Check for new messages list")
	message, _ := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    0,
		Block:    0,
		NoAck:    false,
	}).Result()

	if len(message) > 0 {
		newMessageList := message[0].Messages
		messages = append(messages, newMessageList...)

		log.Printf("New %d messages found", len(newMessageList))

	} else {
		log.Print("Not found new messages")
	}

	return messages
}
