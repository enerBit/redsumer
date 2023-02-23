package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
)

const (
	streamName   string = "test:stream"
	groupName    string = "test:group"
	consumerName string = "test:consumer"
)

func TestWaitForStreamError(t *testing.T) {
	db, mock := redismock.NewClientMock()

	mock.ExpectExists(streamName).SetVal(0)
	mock.ExpectExists(streamName).SetVal(0)
	mock.ExpectExists(streamName).SetVal(0)
	mock.ExpectExists(streamName).SetVal(0)

	err := WaitForStream(context.Background(), db, streamName, []int{1, 2, 3, 4})
	t.Logf("logs: %v", err)
	if err == nil {
		t.Error("should no connect")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestWaitForStreamSuccess(t *testing.T) {
	db, mock := redismock.NewClientMock()

	mock.ExpectExists(streamName).SetVal(0)
	mock.ExpectExists(streamName).SetVal(0)
	mock.ExpectExists(streamName).SetVal(0)
	mock.ExpectExists(streamName).SetVal(1)

	err := WaitForStream(context.Background(), db, streamName, []int{1, 2, 3, 4})
	t.Logf("logs: %v", err)
	if err != nil {
		t.Error(err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestConsume(t *testing.T) {
	db, mock := redismock.NewClientMock()
	ids := []string{"1676389477-0", "1676389497-0"}

	claimArgs := &redis.XClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minIdle,
		Messages: ids,
	}

	readGroupsArgs := &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    0,
		Block:    time.Millisecond * 1,
		NoAck:    false,
	}

	data := map[string]any{"test": "test"}

	resultXpending := []redis.XPendingExt{
		{
			ID:         "1676389477-0",
			Consumer:   consumerName,
			Idle:       minIdle + 1*time.Second,
			RetryCount: 1,
		},
		{
			ID:         "1676389487-0",
			Consumer:   consumerName,
			Idle:       minIdle,
			RetryCount: 2,
		},
		{
			ID:         "1676389497-0",
			Consumer:   consumerName,
			Idle:       3*minIdle + 1*time.Second,
			RetryCount: 3,
		},
	}

	msgA := []redis.XMessage{
		{
			ID:     "1676389477-0",
			Values: data,
		},
		{
			ID:     "1676389497-0",
			Values: data,
		},
	}

	msgB := []redis.XMessage{
		{
			ID:     "1676389466-1",
			Values: data,
		},
	}

	stream := []redis.XStream{
		{
			Stream:   streamName,
			Messages: msgB,
		},
	}

	xPendingArgs := &redis.XPendingExtArgs{
		Stream: streamName,
		Group:  groupName,
		Idle:   minIdle,
		Start:  "-",
		End:    "+",
		Count:  0,
	}

	mock.ExpectXPendingExt(xPendingArgs).SetVal(resultXpending)
	mock.ExpectXClaim(claimArgs).SetVal(msgA)
	mock.ExpectXReadGroup(readGroupsArgs).SetVal(stream)

	ctx := context.Background()
	stream_result, err := Consume(ctx, db, groupName, consumerName, streamName)

	if err != nil {
		t.Error(err)
	}

	t.Log(stream_result)
	if len(stream_result) != 3 {
		t.Fatalf("incomplete result set")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
