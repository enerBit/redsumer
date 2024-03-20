package consumer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
)

const (
	streamName   string = "stream-test"
	groupName    string = "group-test"
	consumerName string = "consumer-test"
)

func TestCreateGroupSuccess(t *testing.T) {
	db, mock := redismock.NewClientMock()

	mock.ExpectExists(streamName).SetVal(1)
	mock.ExpectXGroupCreate(streamName, groupName, FIRST_ID_INSIDE_THE_STREAM).SetVal("OK")

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
			Tries:              []int{1},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	err := consumer.createGroup(context.Background())
	if err != nil {
		t.Error(err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCreateGroupError(t *testing.T) {
	db, mock := redismock.NewClientMock()

	mock.ExpectExists(streamName).SetVal(1)
	mock.ExpectXGroupCreate(streamName, groupName, FIRST_ID_INSIDE_THE_STREAM).SetErr(errors.New("error"))

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
			Tries:              []int{1},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	err := consumer.createGroup(context.Background())
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
			Tries:              []int{1, 2, 3, 4},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	err := consumer.waitForStream(context.Background())
	if err != nil {
		t.Error(err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestWaitForStreamError(t *testing.T) {
	db, mock := redismock.NewClientMock()

	mock.ExpectExists(streamName).SetVal(0)
	mock.ExpectExists(streamName).SetVal(0)
	mock.ExpectExists(streamName).SetVal(0)
	mock.ExpectExists(streamName).SetVal(0)

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
			Tries:              []int{1, 2, 3, 4},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	err := consumer.waitForStream(context.Background())
	if err == nil {
		t.Error("should no connect")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestStillMineSuccess(t *testing.T) {
	db, mock := redismock.NewClientMock()

	idle := time.Second * 1
	messageId := "1676389477-0"

	mock.ExpectXPendingExt(&redis.XPendingExtArgs{
		Stream:   streamName,
		Group:    groupName,
		Idle:     idle,
		Start:    messageId,
		End:      messageId,
		Count:    1,
		Consumer: consumerName,
	}).SetVal([]redis.XPendingExt{{}})

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
			IdleStillMine:      idle,
			Tries:              []int{},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	ok, err := consumer.StillMine(context.Background(), messageId)
	if err != nil {
		t.Error(err)
	}

	if !ok {
		t.Error("should be true")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestStillMineError(t *testing.T) {
	db, mock := redismock.NewClientMock()

	idle := time.Second * 1
	messageId := "1676389477-0"

	mock.ExpectXPendingExt(&redis.XPendingExtArgs{
		Stream:   streamName,
		Group:    groupName,
		Idle:     idle,
		Start:    messageId,
		End:      messageId,
		Count:    1,
		Consumer: consumerName,
	}).SetErr(errors.New("error"))

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
			IdleStillMine:      idle,
			Tries:              []int{},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	ok, err := consumer.StillMine(context.Background(), messageId)
	if err == nil {
		t.Error("should no connect")
	}

	if ok {
		t.Error("should be false")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestStillMineFalse(t *testing.T) {
	db, mock := redismock.NewClientMock()

	idle := time.Second * 1
	messageId := "1676389477-0"

	mock.ExpectXPendingExt(&redis.XPendingExtArgs{
		Stream:   streamName,
		Group:    groupName,
		Idle:     idle,
		Start:    messageId,
		End:      messageId,
		Count:    1,
		Consumer: consumerName,
	}).SetVal([]redis.XPendingExt{})

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
			IdleStillMine:      idle,
			Tries:              []int{},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	ok, err := consumer.StillMine(context.Background(), messageId)
	if err != nil {
		t.Error(err)
	}

	if ok {
		t.Error("should be false")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}

}

func TestAckSuccess(t *testing.T) {
	db, mock := redismock.NewClientMock()

	messageId := "1676389477-0"

	mock.ExpectXAck(streamName, groupName, messageId).SetVal(1)

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

	err := consumer.Ack(context.Background(), messageId)
	if err != nil {
		t.Error(err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestAckError(t *testing.T) {
	db, mock := redismock.NewClientMock()

	messageId := "1676389477-0"

	mock.ExpectXAck(streamName, groupName, messageId).SetErr(errors.New("error"))

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

	err := consumer.Ack(context.Background(), messageId)
	if err == nil {
		t.Error("should no connect")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestNewMessagesSuccess(t *testing.T) {
	db, mock := redismock.NewClientMock()

	var batchSize int64 = 5
	block := time.Millisecond * 1

	readGroupsArgs := &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    batchSize,
		Block:    block,
		NoAck:    false,
	}

	data := map[string]any{"test": "test"}

	msg := []redis.XMessage{
		{
			ID:     "1676389477-0",
			Values: data,
		},
		{
			ID:     "1676383497-0",
			Values: data,
		},
		{
			ID:     "1676289497-0",
			Values: data,
		},
		{
			ID:     "1676189497-0",
			Values: data,
		},
		{
			ID:     "1676369497-0",
			Values: data,
		},
	}

	stream := []redis.XStream{
		{
			Stream:   streamName,
			Messages: msg,
		},
	}

	mock.ExpectXReadGroup(readGroupsArgs).SetVal(stream)

	consumer := &Consumer{
		ConsumerArgs: ConsumerArgs{
			StreamName:         streamName,
			GroupName:          groupName,
			ConsumerName:       consumerName,
			BatchSize:          batchSize,
			ClaimBatchSize:     nil,
			PendingBatchSize:   nil,
			Block:              block,
			MinDurationToClaim: 0,
			IdleStillMine:      0,
			Tries:              []int{},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	msgs, err := consumer.newMessages(context.Background())
	if err != nil {
		t.Error(err)
	}

	if len(msgs) != 5 {
		t.Fatalf("incomplete result set")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestNewMessagesError(t *testing.T) {
	db, mock := redismock.NewClientMock()

	var batchSize int64 = 5
	block := time.Millisecond * 1

	readGroupsArgs := &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    batchSize,
		Block:    block,
		NoAck:    false,
	}

	mock.ExpectXReadGroup(readGroupsArgs).SetErr(errors.New("error"))

	consumer := &Consumer{
		ConsumerArgs: ConsumerArgs{
			StreamName:         streamName,
			GroupName:          groupName,
			ConsumerName:       consumerName,
			BatchSize:          batchSize,
			ClaimBatchSize:     nil,
			PendingBatchSize:   nil,
			Block:              block,
			MinDurationToClaim: 0,
			IdleStillMine:      0,
			Tries:              []int{},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	_, err := consumer.newMessages(context.Background())
	if err == nil {
		t.Error("should no connect")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestPendingMessagesSuccess(t *testing.T) {
	db, mock := redismock.NewClientMock()

	var pendingBatchSize int64 = 5
	block := time.Millisecond * 1

	xReadGroupArgs := &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, "0-0"},
		Count:    pendingBatchSize,
		Block:    block,
		NoAck:    false,
	}

	data := map[string]any{"test": "test"}

	msg := []redis.XMessage{
		{
			ID:     "1676389477-0",
			Values: data,
		},
		{
			ID:     "1676383497-0",
			Values: data,
		},
		{
			ID:     "1676289497-0",
			Values: data,
		},
		{
			ID:     "1676189497-0",
			Values: data,
		},
		{
			ID:     "1676369497-0",
			Values: data,
		},
	}

	stream := []redis.XStream{
		{
			Stream:   streamName,
			Messages: msg,
		},
	}

	mock.ExpectXReadGroup(xReadGroupArgs).SetVal(stream)

	consumer := &Consumer{
		ConsumerArgs: ConsumerArgs{
			StreamName:         streamName,
			GroupName:          groupName,
			ConsumerName:       consumerName,
			BatchSize:          0,
			ClaimBatchSize:     nil,
			PendingBatchSize:   &pendingBatchSize,
			Block:              block,
			MinDurationToClaim: 0,
			IdleStillMine:      0,
			Tries:              []int{},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	msgs, err := consumer.pendingMessages(context.Background())
	if err != nil {
		t.Error(err)
	}

	if len(msgs) != 5 {
		t.Fatalf(fmt.Sprint("incomplete result set ", len(msgs)))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestPendingMessagesError(t *testing.T) {
	db, mock := redismock.NewClientMock()

	var pendingBatchSize int64 = 5
	block := time.Millisecond * 1

	xReadGroupArgs := &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, "0-0"},
		Count:    pendingBatchSize,
		Block:    block,
		NoAck:    false,
	}

	mock.ExpectXReadGroup(xReadGroupArgs).SetErr(errors.New("error"))

	consumer := &Consumer{
		ConsumerArgs: ConsumerArgs{
			StreamName:         streamName,
			GroupName:          groupName,
			ConsumerName:       consumerName,
			BatchSize:          0,
			ClaimBatchSize:     nil,
			PendingBatchSize:   &pendingBatchSize,
			Block:              block,
			MinDurationToClaim: 0,
			IdleStillMine:      0,
			Tries:              []int{},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	_, err := consumer.pendingMessages(context.Background())
	if err == nil {
		t.Error("should no connect")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestClaimedSuccess(t *testing.T) {
	db, mock := redismock.NewClientMock()

	minDurationToClaim := time.Second * 1
	var claimBatchSize int64 = 2

	claimArgs := &redis.XAutoClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minDurationToClaim,
		Start:    FIRST_ID_INSIDE_THE_STREAM,
		Count:    claimBatchSize,
	}

	data := map[string]any{"test": "test"}

	msg := []redis.XMessage{
		{
			ID:     "1676389477-0",
			Values: data,
		},
		{
			ID:     "1676389497-0",
			Values: data,
		},
	}

	mock.ExpectXAutoClaim(claimArgs).SetVal(msg, "OK")

	consumer := &Consumer{
		ConsumerArgs: ConsumerArgs{
			StreamName:         streamName,
			GroupName:          groupName,
			ConsumerName:       consumerName,
			BatchSize:          0,
			ClaimBatchSize:     &claimBatchSize,
			PendingBatchSize:   nil,
			Block:              0,
			MinDurationToClaim: minDurationToClaim,
			IdleStillMine:      0,
			Tries:              []int{},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	msgs, err := consumer.claimedMessages(context.Background())
	if err != nil {
		t.Error(err)
	}

	if len(msgs) != 2 {
		t.Fatalf("incomplete result set")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestClaimedError(t *testing.T) {
	db, mock := redismock.NewClientMock()

	minDurationToClaim := time.Second * 1
	var claimBatchSize int64 = 2

	claimArgs := &redis.XAutoClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minDurationToClaim,
		Start:    FIRST_ID_INSIDE_THE_STREAM,
		Count:    claimBatchSize,
	}

	mock.ExpectXAutoClaim(claimArgs).SetErr(errors.New("error"))

	consumer := &Consumer{
		ConsumerArgs: ConsumerArgs{
			StreamName:         streamName,
			GroupName:          groupName,
			ConsumerName:       consumerName,
			BatchSize:          0,
			ClaimBatchSize:     &claimBatchSize,
			PendingBatchSize:   nil,
			Block:              0,
			MinDurationToClaim: minDurationToClaim,
			IdleStillMine:      0,
			Tries:              []int{},
		},
		client:                 db,
		LatestPendingMessageId: "0-0",
	}

	_, err := consumer.claimedMessages(context.Background())
	if err == nil {
		t.Error("should no connect")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
