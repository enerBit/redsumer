package consumer

import (
	"context"
	"testing"

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

	autoClaimArgs := &redis.XAutoClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minIdle,
		Count:    1000,
		Start:    "0-0",
	}
	readGroupsArgs := &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    0,
		Block:    0,
		NoAck:    false,
	}

	data := map[string]any{"test": "test"}

	msgA := []redis.XMessage{
		{
			ID:     "1676389466-0",
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

	mock.ExpectXAutoClaim(autoClaimArgs).SetVal(msgA, "0-0")
	mock.ExpectXReadGroup(readGroupsArgs).SetVal(stream)

	ctx := context.Background()
	stream_result, err := Consume(ctx, db, groupName, consumerName, streamName)

	if err != nil {
		t.Error(err)
	}

	if len(stream_result) != 2 {
		t.Fatalf("incomplete result set")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
