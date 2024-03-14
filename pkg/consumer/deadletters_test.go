package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
)

func TestDeadLetters(t *testing.T) {
	ctx := context.Background()
	db, mock := redismock.NewClientMock()

	filteredID := "1676389677-0"

	data := map[string]any{"test": "test"}

	pending := []redis.XPendingExt{
		{
			ID:         "1676389477-0",
			Consumer:   "anyOtherConsumer",
			Idle:       minIdle,
			RetryCount: 11,
		},
		{
			ID:         "1676389577-0",
			Consumer:   "anyOtherConsumer",
			Idle:       55 * time.Minute,
			RetryCount: 11,
		},
		{
			ID:         filteredID,
			Consumer:   "anyOtherConsumer",
			Idle:       61 * time.Minute,
			RetryCount: 400,
		},
	}

	mock.ExpectXPendingExt(&redis.XPendingExtArgs{
		Stream: streamName,
		Group:  groupName,
		Idle:   minIdle,
		Start:  "-",
		End:    "+",
		Count:  0,
	}).SetVal(pending)

	mock.ExpectXClaim(&redis.XClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minIdle,
		Messages: []string{filteredID},
	}).SetVal([]redis.XMessage{
		{
			ID:     filteredID,
			Values: data,
		},
	})

	deadStreamName := fmt.Sprintf("dead:letters:%s", streamName)
	mock.ExpectXAdd(&redis.XAddArgs{
		Stream: deadStreamName,
		MaxLen: 0,
		Values: data,
	}).SetVal("11111111-0")

	mock.ExpectXAck(streamName, groupName, filteredID).SetVal(1)

	threshold := 1 * time.Hour

	consumer := &Consumer{
		ConsumerArgs: ConsumerArgs{
			StreamName:         streamName,
			GroupName:          groupName,
			ConsumerName:       consumerName,
			BatchSize:          0,
			ClaimBatchSize:     nil,
			PendingBatchSize:   nil,
			Block:              0,
			MinDurationToClaim: 0,
			IdleStillMine:      0,
			Tries:              []int{},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	err := consumer.AcknowledgeDeadLetters(ctx, deadStreamName, threshold)
	if err != nil {
		t.Errorf("Error while acknowledge messages: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
